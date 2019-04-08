package broker

import (
	"errors"
	"strings"
)

var (
	InvalidMsg        = errors.New("Invalid msg")
	InvalidOp         = errors.New("Invalid msg op")
	InvalidQueue      = errors.New("Invalid msg queue name")
	InvalidTextLength = errors.New("Invalid msg text length")
)

type msg struct {
	op, queue, text string
}

// parse parses the wire msg. Expecting the format ping or op;queue;[text]
func parse(b []byte) (*msg, error) {
	m := &msg{}
	s := strings.TrimSpace(string(b))
	if s == "ping" {
		m.op = "ping"
		return m, nil
	}
	n := strings.Count(s, ";")
	if n != 2 {
		return m, InvalidMsg
	}
	p := strings.Split(s, ";")
	if p[0] != "pub" && p[0] != "sub" {
		return m, InvalidOp
	}
	if p[1] == "" {
		return m, InvalidQueue
	}
	if p[0] == "pub" && p[2] == "" {
		return m, InvalidTextLength
	}
	m.op = p[0]
	m.queue = p[1]
	m.text = p[2]
	return m, nil
}
