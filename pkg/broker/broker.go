package broker

import (
	"errors"
	"fmt"
	"io"
	"net"
)

var subDupeErr = errors.New("Subscriber already exist")

type opFunc func(storage)

type Broker struct {
	ops   chan opFunc
	Debug bool
}

type Conf struct {
	Debug bool
}

// Handle reads from the client reader, parses the msg and runs the op specified
func (b *Broker) Handle(cnt *client) (err error, fatal bool) {
	var (
		buf [128]byte
		n   int
		m   *msg
	)
	n, err = cnt.r.Read(buf[:])
	if timeoutErr, ok := err.(net.Error); ok && timeoutErr.Timeout() {
		b.removeSubs(cnt)
		fatal = true
		return
	}
	if err == io.EOF {
		b.removeSubs(cnt)
		fatal = true
		return
	}
	if err != nil {
		fatal = true
		return
	}
	m, err = parse(buf[:n])
	if err != nil {
		return
	}
	err = b.runOp(m, cnt)
	return
}

// runOp runs the op and saves any subscriptions on the client
func (b *Broker) runOp(m *msg, cnt *client) error {
	if m.op == "ping" {
		return nil
	}
	if m.op == "sub" {
		cnt.subs = append(cnt.subs, m.queue) // TODO: method on cnt
	}
	if m.op == "pub" {
		b.pub(m)
		b.dispatch(m)
		if b.Debug {
			b.storeDump()
		}
		return nil
	}
	if m.op == "sub" {
		err := b.sub(m, cnt)
		if err != nil {
			return err
		}
		b.dispatch(m)
		if b.Debug {
			b.storeDump()
		}
		return nil
	}
	return nil
}

// pub publishes a msg on the specified queue
func (b *Broker) pub(m *msg) {
	b.ops <- func(s storage) {
		s.createQueueIfNotExist(m)
		s.addMsg(m)
	}
}

// sub adds a subscriber to the specified queue
func (b *Broker) sub(m *msg, cnt *client) error {
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

func (b *Broker) dispatch(m *msg) {
	b.ops <- func(s storage) {
		if len(s[m.queue].msgs) == 0 || len(s[m.queue].subs) == 0 {
			return
		}
		go deliver(s.copyQueue(m))
		s.clearMsgs(m)
	}
}

func deliver(q *queue) {
	for _, m := range q.msgs {
		for _, w := range q.subs {
			fmt.Fprintf(w, "%s\n", m)
		}
	}
}

func (b *Broker) removeSubs(cnt *client) {
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
	if b.Debug {
		b.storeDump()
	}
}

func (b *Broker) storeDump() {
	b.ops <- func(s storage) {
		fmt.Printf("storeDump ")
		for k, v := range s {
			fmt.Printf("%s:%d:%d ", k, len(v.msgs), len(v.subs))
		}
		fmt.Println()
	}
}

// listener runs opFuncs on the ops chan
func (b *Broker) listener() {
	s := make(storage)
	for op := range b.ops {
		op(s)
	}
}

// NewBroker starts a listener for the ops chan and returns a Broker
func NewBroker(conf *Conf) *Broker {
	b := &Broker{
		ops:   make(chan opFunc),
		Debug: conf.Debug,
	}
	go b.listener()
	return b
}
