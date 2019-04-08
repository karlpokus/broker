package broker

import (
	"crypto/rand"
	"fmt"
	"io"
)

type client struct {
	id   string
	w    io.Writer
	subs []string
}

func newId() (string, error) {
	var b [16]byte
	n, err := rand.Read(b[:])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:n]), nil
}

func NewClient(w io.Writer) (*client, error) {
	var c client
	id, err := newId()
	if err != nil {
		return &c, err
	}
	c.id = id
	c.w = w
	return &c, nil
}
