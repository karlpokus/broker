package broker

import (
	"crypto/rand"
	"fmt"
	"io"
)

type client struct {
	id   string
	subs []string
	w    io.Writer
	r    io.Reader
}

func (cnt *client) saveSub(m *msg) {
	if m.op == "sub" {
		cnt.subs = append(cnt.subs, m.queue)
	}
}

func newId() (string, error) {
	var b [16]byte
	n, err := rand.Read(b[:])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:n]), nil
}

func NewClient(r io.Reader, w io.Writer) (*client, error) {
	var c client
	id, err := newId()
	if err != nil {
		return &c, err
	}
	c.id = id
	c.r = r
	c.w = w
	return &c, nil
}
