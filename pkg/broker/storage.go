package broker

import "io"

type storage map[string]queue

type queue struct {
	msgs []string
	subs subscribers
}

type subscribers map[string]io.Writer

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

func (s storage) addSub(m *msg, cnt *client) {
	s[m.queue].subs[cnt.id] = cnt.w
}

func (s storage) isDupe(m *msg, cnt *client) bool {
	_, ok := s[m.queue].subs[cnt.id]
	return ok
}

func (s storage) clearMsgs(m *msg) {
	s[m.queue] = queue{
		msgs: []string{},
		subs: s[m.queue].subs,
	}
}

func (s storage) copyQueue(m *msg) *queue {
	subs := make(subscribers)
	for id, w := range s[m.queue].subs {
		subs[id] = w
	}
	return &queue{
		msgs: s[m.queue].msgs,
		subs: subs,
	}
}
