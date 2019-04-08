package server

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/karlpokus/broker/pkg/broker"
)

var bkr *broker.Broker

type server struct{}

func New() server {
	return server{}
}

func (srv server) Start() error {
	l, err := net.Listen("tcp", ":12001")
	if err != nil {
		return err
	}
	fmt.Println("listening..")
	bkr = broker.NewBroker(&broker.Conf{
		Debug: true,
	})
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("connection error: %s\n", err)
			continue
		}
		go handler(conn)
	}
}

func handler(conn net.Conn) {
	defer conn.Close()
	cnt, err := broker.NewClient(conn)
	if err != nil {
		fmt.Printf("Unable to create new client %s\n", err)
		return
	}

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		var buf [128]byte
		n, err := conn.Read(buf[:])
		if err, ok := err.(net.Error); ok && err.Timeout() { // TODO: create wrapper
			bkr.RemoveSubs(cnt)
			return
		}
		if err == io.EOF {
			bkr.RemoveSubs(cnt)
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		err = bkr.Parse(buf[:n], cnt)
		if err != nil {
			fmt.Fprintf(conn, "%s\n", err)
			continue
		}
		fmt.Fprintln(conn, "ok")
	}
}
