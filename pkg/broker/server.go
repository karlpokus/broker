package broker

import (
	"fmt"
	"io"
	"net"
	"time"
)

var bkr *broker

type server struct{}

func NewServer() server {
	return server{}
}

func (srv server) Start() error {
	l, err := net.Listen("tcp", ":12001")
	if err != nil {
		return err
	}
	fmt.Println("listening..")
	bkr = NewBroker(&brokerConf{
		debug: true,
	})
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("connection error: %s\n", err)
		}
		go handler(conn)
	}
}

func handler(conn net.Conn) {
	defer conn.Close()
	id, err := NewId()
	if err != nil {
		fmt.Printf("Unable to create new id %s\n", err) // TODO: better log
		return
	}
	var n int

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		var buf [128]byte
		n, err = conn.Read(buf[:])
		if err, ok := err.(net.Error); ok && err.Timeout() {
			bkr.RemoveClient(id)
			return
		}
		if err == io.EOF {
			bkr.RemoveClient(id)
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		err = bkr.Parse(buf[:n], conn, id)
		if err != nil {
			fmt.Fprintf(conn, "%s\n", err)
			continue
		}
		fmt.Fprintln(conn, "ok")
	}
}
