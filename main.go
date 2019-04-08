package main

import "github.com/karlpokus/broker/pkg/server"

func main() {
	err := server.New().Start()
	if err != nil {
		panic(err)
	}
}
