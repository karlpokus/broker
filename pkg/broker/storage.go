package broker

import "io"

type storage map[string]queue

type queue struct {
	msgs []string
	subs subscribers
}

type subscribers map[uuid]io.Writer

func (s storage) createQueueIfNotExist(m *msg) {
	_, ok := s[m.queue]
	if !ok {
		s[m.queue] = queue{
			subs: make(subscribers),
		}
	}
}

func (s storage) addMsg(m *msg) {
	q, _ := s[m.queue]
	q.msgs = append(q.msgs, m.text)
	s[m.queue] = q
}

func (s storage) addSub(m *msg, w io.Writer, id uuid) {
	s[m.queue].subs[id] = w
}

func (s storage) isDupe(m *msg, id uuid) bool {
	_, ok := s[m.queue].subs[id]
	return ok
}

func (s storage) clearMsgs(m *msg) {
	s[m.queue] = queue{
		msgs: []string{},
		subs: s[m.queue].subs,
	}
}
