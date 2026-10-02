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

// Package grpcmetrics provides gRPC client and server interceptors that export
// the same series as github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
// through the common/metric counters.
//
// The counters of each method are resolved once and cached, so a stream
// message costs a single counter increment instead of a lookup by label values.
package grpcmetrics

import (
	"context"
	"errors"
	"io"
	"maps"
	"strings"

	"github.com/puzpuzpuz/xsync/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/metric"
)

type rpcType string

const (
	unary        rpcType = "unary"
	clientStream rpcType = "client_stream"
	serverStream rpcType = "server_stream"
	bidiStream   rpcType = "bidi_stream"

	// numCodes covers the gRPC status codes, from OK to Unauthenticated.
	numCodes = int(codes.Unauthenticated) + 1
)

type sideNames struct {
	started, handled, msgReceived, msgSent           string
	startedHelp, handledHelp, receivedHelp, sentHelp string
}

var (
	serverNames = sideNames{
		started:      "grpc_server_started_total",
		startedHelp:  "Total number of RPCs started on the server.",
		handled:      "grpc_server_handled_total",
		handledHelp:  "Total number of RPCs completed on the server, regardless of success or failure.",
		msgReceived:  "grpc_server_msg_received_total",
		receivedHelp: "Total number of RPC stream messages received on the server.",
		msgSent:      "grpc_server_msg_sent_total",
		sentHelp:     "Total number of gRPC stream messages sent by the server.",
	}
	clientNames = sideNames{
		started:      "grpc_client_started_total",
		startedHelp:  "Total number of RPCs started on the client.",
		handled:      "grpc_client_handled_total",
		handledHelp:  "Total number of RPCs completed by the client, regardless of success or failure.",
		msgReceived:  "grpc_client_msg_received_total",
		receivedHelp: "Total number of RPC stream messages received by the client.",
		msgSent:      "grpc_client_msg_sent_total",
		sentHelp:     "Total number of gRPC stream messages sent by the client.",
	}

	methods = xsync.NewMap[methodKey, *methodMetrics]()
)

type methodKey struct {
	client     bool
	rpcType    rpcType
	fullMethod string
}

// methodMetrics holds the counters of one method, on one side of the call.
type methodMetrics struct {
	labels   map[string]any
	names    *sideNames
	started  metric.Counter
	received metric.Counter
	sent     metric.Counter
	handled  [numCodes]metric.Counter
}

func newMethodMetrics(key methodKey) *methodMetrics {
	names := &serverNames
	if key.client {
		names = &clientNames
	}
	service, method := splitMethodName(key.fullMethod)
	labels := map[string]any{
		"grpc_type":    string(key.rpcType),
		"grpc_service": service,
		"grpc_method":  method,
	}
	m := &methodMetrics{
		labels:   labels,
		names:    names,
		started:  metric.NewCounter(names.started, names.startedHelp, metric.Dimensionless, labels),
		received: metric.NewCounter(names.msgReceived, names.receivedHelp, metric.Dimensionless, labels),
		sent:     metric.NewCounter(names.msgSent, names.sentHelp, metric.Dimensionless, labels),
	}
	for code := range m.handled {
		m.handled[code] = m.newHandled(codes.Code(code))
	}
	return m
}

func (m *methodMetrics) newHandled(code codes.Code) metric.Counter {
	labels := maps.Clone(m.labels)
	labels["grpc_code"] = code.String()
	return metric.NewCounter(m.names.handled, m.names.handledHelp, metric.Dimensionless, labels)
}

func getMethodMetrics(client bool, t rpcType, fullMethod string) *methodMetrics {
	key := methodKey{client: client, rpcType: t, fullMethod: fullMethod}
	if m, ok := methods.Load(key); ok {
		return m
	}
	m, _ := methods.LoadOrCompute(key, func() (*methodMetrics, bool) {
		return newMethodMetrics(key), false
	})
	return m
}

func (m *methodMetrics) handledWith(err error) {
	st, _ := status.FromError(err)
	if code := st.Code(); int(code) < numCodes {
		m.handled[code].Inc()
	} else {
		m.newHandled(code).Inc()
	}
}

// Register pre-initializes the started, handled (for every code), msg_sent and
// msg_received series of every method registered on server, so that they are
// exported with a zero value before the first call.
func Register(server *grpc.Server) {
	for service, info := range server.GetServiceInfo() {
		for _, mi := range info.Methods {
			m := getMethodMetrics(false, streamType(mi.IsClientStream, mi.IsServerStream), "/"+service+"/"+mi.Name)
			m.started.Add(0)
			m.received.Add(0)
			m.sent.Add(0)
			for _, c := range m.handled {
				c.Add(0)
			}
		}
	}
}

func start(client bool, t rpcType, fullMethod string) *methodMetrics {
	m := getMethodMetrics(client, t, fullMethod)
	m.started.Inc()
	return m
}

// UnaryServerInterceptor records the metrics of a unary RPC on the server.
func UnaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (any, error) {
	r := start(false, unary, info.FullMethod)
	r.received.Inc()
	resp, err := handler(ctx, req)
	r.handledWith(err)
	if err == nil {
		r.sent.Inc()
	}
	return resp, err
}

// StreamServerInterceptor records the metrics of a streaming RPC on the server.
func StreamServerInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) error {
	r := start(false, streamType(info.IsClientStream, info.IsServerStream), info.FullMethod)
	err := handler(srv, &monitoredServerStream{ServerStream: ss, metrics: r})
	r.handledWith(err)
	return err
}

type monitoredServerStream struct {
	grpc.ServerStream
	metrics *methodMetrics
}

func (s *monitoredServerStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.metrics.sent.Inc()
	}
	return err
}

func (s *monitoredServerStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.metrics.received.Inc()
	}
	return err
}

// UnaryClientInterceptor records the metrics of a unary RPC on the client.
func UnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	r := start(true, unary, method)
	r.sent.Inc()
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err == nil {
		r.received.Inc()
	}
	r.handledWith(err)
	return err
}

// StreamClientInterceptor records the metrics of a streaming RPC on the client.
func StreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
	streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	r := start(true, streamType(desc.ClientStreams, desc.ServerStreams), method)
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		r.handledWith(err)
		return nil, err
	}
	return &monitoredClientStream{ClientStream: cs, metrics: r}, nil
}

type monitoredClientStream struct {
	grpc.ClientStream
	metrics *methodMetrics
}

func (s *monitoredClientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err == nil {
		s.metrics.sent.Inc()
	}
	return err
}

func (s *monitoredClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	switch {
	case err == nil:
		s.metrics.received.Inc()
	case errors.Is(err, io.EOF):
		s.metrics.handledWith(nil)
	default:
		s.metrics.handledWith(err)
	}
	return err
}

func streamType(isClientStream, isServerStream bool) rpcType {
	switch {
	case !isClientStream && !isServerStream:
		return unary
	case isClientStream && !isServerStream:
		return clientStream
	case !isClientStream && isServerStream:
		return serverStream
	default:
		return bidiStream
	}
}

func splitMethodName(fullMethod string) (service, method string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	if i := strings.Index(fullMethod, "/"); i >= 0 {
		return fullMethod[:i], fullMethod[i+1:]
	}
	return "unknown", "unknown"
}
