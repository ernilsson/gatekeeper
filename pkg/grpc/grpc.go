package grpc

import (
	"context"
	"crypto/tls"
	"github.com/ernilsson/gatekeeper/internal/pb"
	"google.golang.org/grpc"
)

func Start(port string) error {
	lis, err := tls.Listen("tcp", ":"+port, &tls.Config{})
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	pb.RegisterAuthorizationServer(srv, authorization{})
	return srv.Serve(lis)
}

type authorization struct {
	pb.AuthorizationServer
}

func (a authorization) Authorize(ctx context.Context, msg *pb.AuthorizationRequest) (*pb.AuthorizationResponse, error) {
	panic("not implemented")
}
