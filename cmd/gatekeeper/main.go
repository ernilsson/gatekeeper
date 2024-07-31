package main

import "github.com/ernilsson/gatekeeper/pkg/grpc"

func main() {
	if err := grpc.Start(":8080"); err != nil {
		panic(err)
	}
}
