package broker

import (
	"errors"
	"fmt"
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

// Parse parses the wire msg, saves any subscriptions on the client and runs the op
func (b *broker) Parse(buf []byte, cnt *client) error {
	m, err := parse(buf)
	if err != nil {
		return err
	}
	if m.op == "ping" {
		return nil
	}
	if m.op == "sub" { // TODO: method on cnt
		cnt.subs = append(cnt.subs, m.queue)
	}
	err = b.runOp(m, cnt)
	if err != nil {
		return err
	}
	return nil
}

// runOp runs the op specified in the msg
func (b *broker) runOp(m *msg, cnt *client) error {
	if m.op == "pub" {
		b.pub(m)
		b.dispatch(m)
		return nil
	}
	if m.op == "sub" {
		err := b.sub(m, cnt)
		if err != nil {
			return err
		}
		b.dispatch(m)
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
func (b *broker) sub(m *msg, cnt *client) error {
	fail := make(chan error)
	b.ops <- func(s storage) {
		s.createQueueIfNotExist(m)
		if s.isDupe(m, cnt) {
			fail <- subDupeErr
			return
		}
		s.addSub(m, cnt)
		fail <- nil
	}
	return <-fail
}

func (b *broker) dispatch(m *msg) {
	b.ops <- func(s storage) {
		if len(s[m.queue].msgs) == 0 {
			return
		}
		go deliver(s.copyQueue(m))
		s.clearMsgs(m)
	}
}

func deliver(q *queue) {
	for _, m := range q.msgs {
		for _, w := range q.subs {
			fmt.Fprintf(w, "%s", m)
		}
	}
}

func (b *broker) removeSubs(cnt *client) {
	if len(cnt.subs) == 0 {
		return
	}
	b.ops <- func(s storage) {
		for _, q := range cnt.subs { // TODO: method on storage
			_, ok := s[q].subs[cnt.id]
			if ok {
				delete(s[q].subs, cnt.id)
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
