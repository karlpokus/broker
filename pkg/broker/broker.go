package broker

import (
	"errors"
	"fmt"
	"io"
)

var subDupeErr = errors.New("Subscriber already exist")

type opFunc func(storage)

type broker struct {
	ops   chan opFunc
	debug bool
}

type brokerConf struct {
	debug bool
}

// Parse parses the wire msg and runs the op
func (b *broker) Parse(buf []byte, w io.Writer, id uuid) error {
	m, err := parse(buf)
	if err != nil {
		return err
	}
	if m.op == "pub" {
		b.pub(m)
		b.ops <- dispatch(m.queue)
		return nil
	}
	if m.op == "sub" {
		err = b.sub(m, w, id)
		if err != nil {
			return err
		}
		b.ops <- dispatch(m.queue)
		return nil
	}
	return nil
}

// pub publishes a msg on the specified queue
func (b *broker) pub(m *msg) {
	b.ops <- func(s storage) {
		s.createQueueIfNotExist(m)
		s.addMsg(m)
	}
}

// sub adds a subscriber to the specified queue
func (b *broker) sub(m *msg, w io.Writer, id uuid) error {
	fail := make(chan error)
	b.ops <- func(s storage) {
		s.createQueueIfNotExist(m)
		if s.isDupe(m, id) {
			fail <- subDupeErr
			return
		}
		s.addSub(m, w, id)
		fail <- nil
	}
	return <-fail
}

func dispatch(q string) opFunc {
	return func(s storage) {
		if len(s[q].msgs) == 0 {
			return
		}
		go deliver(s[q])
		s[q] = queue{
			msgs: []string{},
			subs: s[q].subs,
		}
	}
}

func deliver(q queue) {
	for _, m := range q.msgs {
		for _, w := range q.subs {
			fmt.Fprintf(w, "%s", m) // TODO: handle returned error
		}
	}
}

func (b *broker) RemoveClient(id uuid) {
	b.ops <- func(s storage) {
		for k, q := range s {
			_, ok := q.subs[id]
			if ok {
				delete(q.subs, id)
				s[k] = q // needed?
			}
		}
	}
}

func debug(s storage) {
	for k, v := range s {
		fmt.Printf("%s: %d msgs, %d subs\n", k, len(v.msgs), len(v.subs))
	}
}

// listener runs opFuncs on the ops chan
func (b *broker) listener() {
	s := make(storage)
	for op := range b.ops {
		op(s)
		if b.debug {
			debug(s)
		}
	}
}

// NewBroker starts a listener for the ops chan and returns a broker
func NewBroker(conf *brokerConf) *broker {
	b := &broker{
		ops:   make(chan opFunc),
		debug: conf.debug,
	}
	go b.listener()
	return b
}
