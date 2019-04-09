package server

import (
	"fmt"
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
	cnt, err := broker.NewClient(conn, conn)
	if err != nil {
		fmt.Printf("Unable to create new client %s\n", err)
		return
	}
	var fatal bool

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second)) // TODO: ttl in server conf
		err, fatal = bkr.Handle(cnt)
		if fatal {
			return
		}
		if err != nil { // if !fatal then err is safe to return to caller
			fmt.Fprintf(conn, "%s\n", err)
			continue
		}
		fmt.Fprintln(conn, "ok")
	}
}
