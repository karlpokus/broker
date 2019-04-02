package broker

import (
	"fmt"
	"net"
)

type queue struct {
	msgs []string
	subs map[net.Addr]net.Conn
}

type storage map[string]queue

type opFunc func(storage)

type broker struct {
	ops chan opFunc
}

func (b *broker) Parse(buf []byte, conn net.Conn) error {
	m, err := parse(buf)
	if err != nil {
		return err
	}
	if m.op == "pub" {
		b.pub(m)
		return nil
	}
	if m.op == "sub" {
		b.sub(m, conn)
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
				subs: make(map[net.Addr]net.Conn),
			}
			return
		}
		q.msgs = append(q.msgs, m.text)
		s[m.queue] = q
	}
	b.ops <- debug
	b.ops <- dispatch(m.queue)
}

func (b *broker) sub(m *msg, conn net.Conn) {
	b.ops <- func(s storage) {
		q, ok := s[m.queue]
		if !ok {
			q = queue{
				subs: make(map[net.Addr]net.Conn),
			}
		}
		q.subs[conn.RemoteAddr()] = conn
		s[m.queue] = q
	}
	b.ops <- debug
	b.ops <- dispatch(m.queue)
}

func dispatch(q string) opFunc {
	return func(s storage) {
		for _, m := range s[q].msgs { // TODO: delete delivered msgs
			for _, conn := range s[q].subs {
				fmt.Fprintf(conn, "%s", m)
			}
		}
	}
}

func (b *broker) RemoveClient(conn net.Conn) {
	b.ops <- func(s storage) {
		for k, q := range s {
			_, ok := q.subs[conn.RemoteAddr()]
			if ok {
				delete(q.subs, conn.RemoteAddr())
				s[k] = q // needed?
			}
		}
	}
	b.ops <- debug
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
func NewBroker() *broker {
	b := &broker{
		ops: make(chan opFunc),
	}
	go b.listener()
	return b
}
