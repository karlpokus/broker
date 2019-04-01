package main

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/karlpokus/broker/pkg/broker"
)

var b = broker.New()

func handler(conn net.Conn) {
	defer conn.Close()

	for {
		conn.SetDeadline(time.Now().Add(2 * time.Minute))
		var buf [128]byte
		n, err := conn.Read(buf[:])
		if err, ok := err.(net.Error); ok && err.Timeout() {
			fmt.Println("client timed out") // TODO: remove client
			return
		}
		if err == io.EOF {
			fmt.Println("client disconnected") // TODO: remove client
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		err = b.Parse(buf[:n], conn)
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
