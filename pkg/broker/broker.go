package broker

import (
	"fmt"
	"io"
)

type queue struct {
	msgs []string
	subs map[uuid]io.Writer
}

type storage map[string]queue

type opFunc func(storage)

type broker struct {
	ops   chan opFunc
	debug bool
}

type brokerConf struct {
	debug bool
}

func (b *broker) Parse(buf []byte, w io.Writer, id uuid) error {
	m, err := parse(buf)
	if err != nil {
		return err
	}
	if m.op == "pub" {
		b.pub(m)
		return nil
	}
	if m.op == "sub" {
		b.sub(m, w, id)
		return nil
	}
	return nil
}

func (b *broker) pub(m *msg) {
	b.ops <- func(s storage) {
		q, ok := s[m.queue]
		if !ok {
			s[m.queue] = queue{
				msgs: []string{m.text},
				subs: make(map[uuid]io.Writer),
			}
			return
		}
		q.msgs = append(q.msgs, m.text)
		s[m.queue] = q
	}
	if b.debug {
		b.ops <- debug
	}
	b.ops <- dispatch(m.queue)
}

func (b *broker) sub(m *msg, w io.Writer, id uuid) {
	b.ops <- func(s storage) {
		q, ok := s[m.queue]
		if !ok {
			q = queue{
				subs: make(map[uuid]io.Writer),
			}
		}
		q.subs[id] = w
		s[m.queue] = q
	}
	if b.debug {
		b.ops <- debug
	}
	b.ops <- dispatch(m.queue)
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
	if b.debug {
		b.ops <- debug
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
