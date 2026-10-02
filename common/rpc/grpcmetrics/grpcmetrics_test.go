// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcmetrics

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/oxia-db/oxia/common/metric"
)

const testService = "grpc.testing.TestService"

type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (*testServer) EmptyCall(context.Context, *testpb.Empty) (*testpb.Empty, error) {
	return &testpb.Empty{}, nil
}

func (*testServer) UnaryCall(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return nil, status.Error(codes.InvalidArgument, "invalid")
}

func (*testServer) StreamingOutputCall(req *testpb.StreamingOutputCallRequest,
	stream grpc.ServerStreamingServer[testpb.StreamingOutputCallResponse]) error {
	for range req.GetResponseParameters() {
		if err := stream.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
			return err
		}
	}
	return nil
}

func (*testServer) FullDuplexCall(
	stream grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
	for {
		if _, err := stream.Recv(); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if err := stream.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
			return err
		}
	}
}

// withReader routes the counters to a fresh SDK meter for the duration of the
// test and returns the reader to collect them from.
func withReader(tb testing.TB) *sdkmetric.ManualReader {
	tb.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := metric.GetMeter()
	metric.SetMeter(provider.Meter("test"))
	methods = xsync.NewMap[methodKey, *methodMetrics]()
	tb.Cleanup(func() {
		metric.SetMeter(previous)
		methods = xsync.NewMap[methodKey, *methodMetrics]()
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

func newTestClient(t *testing.T) (testpb.TestServiceClient, *sdkmetric.ManualReader) {
	t.Helper()
	reader := withReader(t)

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(StreamServerInterceptor),
	)
	testpb.RegisterTestServiceServer(server, &testServer{})
	Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(StreamClientInterceptor),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return testpb.NewTestServiceClient(conn), reader
}

// series returns the value of every exported series of the given metric, keyed
// by its encoded label set.
func series(t *testing.T, reader *sdkmetric.ManualReader, name string) (map[string]int64, string) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	values := map[string]int64{}
	description := ""
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			description = m.Description
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				values[dp.Attributes.Encoded(attribute.DefaultEncoder())] = dp.Value
			}
		}
	}
	return values, description
}

func labelSet(rpc rpcType, method string, code ...codes.Code) string {
	kvs := []attribute.KeyValue{
		attribute.String("grpc_type", string(rpc)),
		attribute.String("grpc_service", testService),
		attribute.String("grpc_method", method),
	}
	if len(code) > 0 {
		kvs = append(kvs, attribute.String("grpc_code", code[0].String()))
	}
	set := attribute.NewSet(kvs...)
	return set.Encoded(attribute.DefaultEncoder())
}

type expected struct {
	started, received, sent int64
	code                    codes.Code
}

func assertMetrics(t *testing.T, reader *sdkmetric.ManualReader, side string, rpc rpcType, method string,
	e expected) {
	t.Helper()
	value := func(name string, labels string) int64 {
		values, _ := series(t, reader, "grpc_"+side+"_"+name+"_total")
		return values[labels]
	}
	labels := labelSet(rpc, method)
	assert.Equal(t, e.started, value("started", labels), "started")
	assert.Equal(t, e.received, value("msg_received", labels), "msg_received")
	assert.Equal(t, e.sent, value("msg_sent", labels), "msg_sent")
	assert.Equal(t, e.started, value("handled", labelSet(rpc, method, e.code)), "handled")
}

func TestUnary(t *testing.T) {
	client, reader := newTestClient(t)

	_, err := client.EmptyCall(t.Context(), &testpb.Empty{})
	require.NoError(t, err)
	assertMetrics(t, reader, "server", unary, "EmptyCall", expected{started: 1, received: 1, sent: 1, code: codes.OK})
	assertMetrics(t, reader, "client", unary, "EmptyCall", expected{started: 1, received: 1, sent: 1, code: codes.OK})

	_, err = client.UnaryCall(t.Context(), &testpb.SimpleRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	assertMetrics(t, reader, "server", unary, "UnaryCall", expected{started: 1, received: 1, code: codes.InvalidArgument})
	assertMetrics(t, reader, "client", unary, "UnaryCall", expected{started: 1, sent: 1, code: codes.InvalidArgument})
}

func TestServerStream(t *testing.T) {
	client, reader := newTestClient(t)

	stream, err := client.StreamingOutputCall(t.Context(), &testpb.StreamingOutputCallRequest{
		ResponseParameters: make([]*testpb.ResponseParameters, 3),
	})
	require.NoError(t, err)
	for range 3 {
		_, err = stream.Recv()
		require.NoError(t, err)
	}
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	assertMetrics(t, reader, "server", serverStream, "StreamingOutputCall",
		expected{started: 1, received: 1, sent: 3, code: codes.OK})
	assertMetrics(t, reader, "client", serverStream, "StreamingOutputCall",
		expected{started: 1, received: 3, sent: 1, code: codes.OK})
}

func TestBidiStream(t *testing.T) {
	client, reader := newTestClient(t)

	stream, err := client.FullDuplexCall(t.Context())
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, stream.Send(&testpb.StreamingOutputCallRequest{}))
		_, err = stream.Recv()
		require.NoError(t, err)
	}
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	assertMetrics(t, reader, "server", bidiStream, "FullDuplexCall",
		expected{started: 1, received: 2, sent: 2, code: codes.OK})
	assertMetrics(t, reader, "client", bidiStream, "FullDuplexCall",
		expected{started: 1, received: 2, sent: 2, code: codes.OK})
}

func TestRegisterPreInitializesSeries(t *testing.T) {
	_, reader := newTestClient(t)

	methods := len(testpb.TestService_ServiceDesc.Methods) + len(testpb.TestService_ServiceDesc.Streams)
	for name, help := range map[string]string{
		"grpc_server_started_total":      "Total number of RPCs started on the server.",
		"grpc_server_msg_sent_total":     "Total number of gRPC stream messages sent by the server.",
		"grpc_server_msg_received_total": "Total number of RPC stream messages received on the server.",
	} {
		values, description := series(t, reader, name)
		assert.Equal(t, help, description, name)
		assert.Len(t, values, methods, name)
	}
	values, description := series(t, reader, "grpc_server_handled_total")
	assert.Equal(t, "Total number of RPCs completed on the server, regardless of success or failure.", description)
	assert.Len(t, values, methods*numCodes)
	assert.Contains(t, values, labelSet(clientStream, "StreamingInputCall", codes.DataLoss))

	// Client series only appear once a call is made.
	values, _ = series(t, reader, "grpc_client_started_total")
	assert.Empty(t, values)
}

type nopServerStream struct {
	grpc.ServerStream
}

func (nopServerStream) SendMsg(any) error { return nil }

// BenchmarkStreamMsgSent measures the metrics cost of a stream message, whose
// counter is resolved once per stream.
func BenchmarkStreamMsgSent(b *testing.B) {
	withReader(b)
	stream := &monitoredServerStream{
		ServerStream: nopServerStream{},
		metrics:      getMethodMetrics(false, bidiStream, "/replication.OxiaLogReplication/Replicate"),
	}

	b.Run("stream", func(b *testing.B) {
		for b.Loop() {
			_ = stream.SendMsg(nil)
		}
	})
	b.Run("stream-parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = stream.SendMsg(nil)
			}
		})
	})
}
