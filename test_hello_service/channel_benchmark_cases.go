package test_hello_service

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/aclements/go-moremath/stats"
	"github.com/aclements/go-moremath/vec"
	"github.com/loov/hrtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RunChannelBenchmarkCases runs numerous test cases to exercise the behavior of the
// given channel. The server side of the channel needs to have a *TestServer (in
// this package) registered to provide the implementation of fsgrpc.TestService
// (proto in this package). If the channel does not support full-duplex
// communication, it must provide at least half-duplex support for bidirectional
// streams.
//
// The test cases will be defined as child tests by invoking t.Run on the given
// *testing.T.

func RunChannelBenchmarkCases(b *testing.B, ch grpc.ClientConnInterface, supportsFullDuplex bool) {
	cli := NewTestServiceClient(ch)

	b.Run("hello", func(b *testing.B) { BenchmarkHelloHistogram(b, cli) })
	// b.Run("hello", func(b *testing.B) { BenchmarkUnaryLatency(b, cli) })
	// b.SetParallelism(1)
	// b.RunParallel(func(pb *testing.PB) { BenchmarkUnaryLatencyParallel(pb, cli) })

}

func BenchmarkUnaryLatencyParallel(pab *testing.PB, cli TestServiceClient) {
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.MD{})

	var name = defaultName

	for pab.Next() {
		req := &HelloRequest{Name: name}
		rsp, err := cli.SayHello(ctx, req)

		if err != nil {
			break
		}
		if rsp != nil {
			break
		}

	}
}

func BenchmarkUnaryLatency(b *testing.B, cli TestServiceClient) {
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.MD{})

	name := defaultName
	for i := 0; i < b.N; i++ {
		req := &HelloRequest{Name: name}
		rsp, err := cli.SayHello(ctx, req)
		if err != nil {
			b.Fatalf("RPC failed: %v", err)
		}
		if !bytes.Equal(testPayloadHello, []byte(rsp.GetMessage())) {
			b.Fatalf("wrong payload returned: expecting %v; got %v", testPayloadHello, rsp.GetMessage())
		}
		// checkRequestHeadersBench(b, testOutgoingMd, rsp.Headers)

		// checkMetadataBench(b, testMdHeaders, hdr, "header")
		// checkMetadataBench(b, testMdTrailers, tlr, "trailer")

	}
}

func BenchmarkHelloHistogram(b *testing.B, cli TestServiceClient) {

	bench := hrtime.NewBenchmark(200000)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.MD{})

	// name := flag.String("histo_name", defaultName, "Name to greet")

	// b.Run("success", func(b *testing.B) {
	for bench.Next() {
		req := &HelloRequest{Name: defaultName}
		rsp, err := cli.SayHello(ctx, req)
		if err != nil {
			b.Fatalf("RPC failed: %v", err)
		}
		if !bytes.Equal(testPayloadHello, []byte(rsp.GetMessage())) {
			b.Fatalf("wrong payload returned: expecting %v; got %v", testPayloadHello, rsp.GetMessage())
		}
	}

	fmt.Println(bench.Histogram(15))

	runs := bench.Float64s()

	fmt.Printf("Mean: %f\n", stats.Mean(runs)*0.001)
	fmt.Printf("StdDev: %f\n", stats.StdDev(runs)*0.001)
	fmt.Printf("NumElements: %d\n", len(runs))
	fmt.Printf("Time in Microsecondes: %d \n", b.Elapsed().Microseconds())
	fmt.Printf("Time in Seconds: %f \n", b.Elapsed().Seconds())
	fmt.Printf("Sanity Check Time Microseconds: %f \n", vec.Sum(runs)*0.001)
	fmt.Printf("Throughput: %f \n", float64(len(runs))/b.Elapsed().Seconds())

	bench.Laps()
	// fmt.Printf("Runs: %v :", bench.Laps())

}
