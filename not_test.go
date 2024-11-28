package grpc_test

import (
	"log"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/transport"
	"google.golang.org/grpc/test_hello_service"
)

// func TestNotnetsConn(t *testing.T) {

// 	service := &test_hello_service.TestServer{}
// 	server := grpc.NewServer()
// 	server.RegisterService(&test_hello_service.TestService_ServiceDesc,service)

// 	lis := transport.NotnetsListen("localhost:50051")
// 	go server.NotnetsServe(lis)

// 	cc, err := grpc.NewClient("localhost:50051")
// 	if err != nil {
// 		log.Fatalf("did not connect: %v", err)
// 	}

// 	test_hello_service.RunChannelBenchmarkCases(t, cc, true)

// 	server.Stop()
// }

func BenchmarkNotnets(b *testing.B) {

	service := &test_hello_service.TestServer{}
	server := grpc.NewServer()
	server.RegisterService(&test_hello_service.TestService_ServiceDesc,service)

	lis := transport.NotnetsListen("50051")
	go server.NotnetsServe(lis)

	cc, err := grpc.NewClient(":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	test_hello_service.RunChannelBenchmarkCases(b, cc, true)
 

	server.Stop()
}