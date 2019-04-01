package main

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/karlpokus/broker/pkg/broker"
)

var bkr = broker.New()

func handler(conn net.Conn) {
	defer conn.Close()

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		var buf [128]byte
		n, err := conn.Read(buf[:])
		if err, ok := err.(net.Error); ok && err.Timeout() {
			bkr.RemoveClient(conn)
			return
		}
		if err == io.EOF {
			bkr.RemoveClient(conn)
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		err = bkr.Parse(buf[:n], conn)
		if err != nil {
			fmt.Fprintf(conn, "%s\n", err.Error())
			continue
		}
		fmt.Fprintln(conn, "ok")
	}
}

func main() {
	l, err := net.Listen("tcp", ":12001")
	if err != nil {
		panic(err)
	}
	fmt.Println("running")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("connection error: %s\n", err)
		}
		go handler(conn)
	}
}
