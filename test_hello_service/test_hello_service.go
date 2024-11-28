package test_hello_service

// protoc --go_out=./ --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative test_hello.proto

import (
	"context"
)

// TestServer has default responses to the various kinds of methods.
type TestServer struct {
	UnimplementedTestServiceServer
}

func (s *TestServer) SayHello(ctx context.Context, in *HelloRequest) (*HelloReply, error) {
	return &HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *TestServer) SayGoodbye(ctx context.Context, in *GoodbyeRequest) (*GoodbyeReply, error) {
	return &GoodbyeReply{Message: "Goodbye " + in.GetName()}, nil
}
