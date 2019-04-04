package broker

import (
	"errors"
	"fmt"
	"io"
)

var subDupe = errors.New("Subscriber already exist")

type storage map[string]queue

type queue struct {
	msgs []string
	subs subscribers
}

type subscribers map[uuid]io.Writer

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
// it creates the queue if it does not exist
func (b *broker) pub(m *msg) {
	b.ops <- func(s storage) {
		q, ok := s[m.queue]
		if !ok {
			s[m.queue] = queue{
				msgs: []string{m.text},
				subs: make(subscribers),
			}
			return
		}
		q.msgs = append(q.msgs, m.text)
		s[m.queue] = q
	}
}

// sub adds a subscriber to the specified queue
// it also checks for dupes
// it creates the queue if it does not exist
func (b *broker) sub(m *msg, w io.Writer, id uuid) error {
	fail := make(chan error)
	b.ops <- func(s storage) {
		q, ok := s[m.queue]
		if !ok {
			q = queue{
				subs: make(subscribers),
			}
		}
		if _, ok := s[m.queue].subs[id]; ok {
			fail <- subDupe
			return
		}
		q.subs[id] = w
		s[m.queue] = q
		fail <- nil
	}
	return <-fail
}

func dispatch(q string) opFunc {
	return func(s storage) {
		for _, m := range s[q].msgs { // TODO: delete delivered msgs
			for _, w := range s[q].subs {
				fmt.Fprintf(w, "%s", m) // TODO: handle returned error
			}
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
