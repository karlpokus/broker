package broker

import (
	"errors"
	"strings"
)

var (
	InvalidMsg   = errors.New("Invalid msg")
	InvalidOp    = errors.New("Invalid msg op")
	InvalidQueue = errors.New("Invalid msg queue name")
)

type msg struct {
	op, queue, text string
}

func parse(b []byte) (*msg, error) {
	m := &msg{}
	s := string(b)
	n := strings.Count(s, ";")
	if n != 2 {
		return m, InvalidMsg
	}
	if s == "ack;;" {
		m.op = "ack"
		return m, nil
	}
	p := strings.Split(s, ";")
	if p[0] != "pub" && p[0] != "sub" {
		return m, InvalidOp
	}
	if p[1] == "" {
		return m, InvalidQueue
	}
	m.op = p[0]
	m.queue = p[1]
	m.text = p[2]
	return m, nil
}
