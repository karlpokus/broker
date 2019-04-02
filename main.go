package main

import "github.com/karlpokus/broker/pkg/broker"

func main() {
	err := broker.NewServer().Start()
	if err != nil {
		panic(err)
	}
}
