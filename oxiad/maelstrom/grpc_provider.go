// Copyright 2023-2025 The Oxia Authors
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

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/rpc/auth"

	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/proto"
)

const (
	oxiaCoordination   = "replication.OxiaCoordination"
	oxiaLogReplication = "replication.OxiaLogReplication"
	oxiaClient         = "io.oxia.proto.v1.OxiaClient"
)

type maelstromGrpcProvider struct {
	sync.Mutex
	services map[string]any

	replicateStreams map[string]*maelstromReplicateServerStream
}

func newMaelstromGrpcProvider() *maelstromGrpcProvider {
	return &maelstromGrpcProvider{
		services:         make(map[string]any),
		replicateStreams: make(map[string]*maelstromReplicateServerStream),
	}
}

func (m *maelstromGrpcProvider) StartGrpcServer(name, _ string, registerFunc func(grpc.ServiceRegistrar),
	_ *tls.Config, _ *auth.Options) (rpc.GrpcServer, error) {
	slog.Info(
		"Start Grpc server",
		slog.String("name", name),
	)

	registerFunc(m)
	return &maelstromGrpcServer{}, nil
}

func (m *maelstromGrpcProvider) RegisterService(desc *grpc.ServiceDesc, impl any) {
	slog.Info(
		"RegisterService",
		slog.String("service-name", desc.ServiceName),
	)
	m.services[desc.ServiceName] = impl
}

func (m *maelstromGrpcProvider) HandleOxiaRequest(msgType MsgType, msg *Message[OxiaMessage], message pb.Message) {
	switch msgType {
	case MsgTypeNewTermRequest:
		if fr, err := m.getService(oxiaCoordination).(proto.OxiaCoordinationServer).NewTerm(context.Background(), message.(*proto.NewTermRequest)); err != nil {
			sendError(msg.Body.MsgId, msg.Src, err)
		} else {
			m.sendResponse(msg, MsgTypeNewTermResponse, fr)
		}

	case MsgTypeBecomeLeaderRequest:
		if blr, err := m.getService(oxiaCoordination).(proto.OxiaCoordinationServer).BecomeLeader(context.Background(), message.(*proto.BecomeLeaderRequest)); err != nil {
			sendError(msg.Body.MsgId, msg.Src, err)
		} else {
			m.sendResponse(msg, MsgTypeBecomeLeaderResponse, blr)
		}

	case MsgTypeTruncateRequest:
		if tr, err := m.getService(oxiaLogReplication).(proto.OxiaLogReplicationServer).Truncate(context.Background(), message.(*proto.TruncateRequest)); err != nil {
			sendError(msg.Body.MsgId, msg.Src, err)
		} else {
			m.sendResponse(msg, MsgTypeTruncateResponse, tr)
		}

	case MsgTypeGetStatusRequest:
		if gsr, err := m.getService(oxiaCoordination).(proto.OxiaCoordinationServer).GetStatus(context.Background(), message.(*proto.GetStatusRequest)); err != nil {
			sendError(msg.Body.MsgId, msg.Src, err)
		} else {
			m.sendResponse(msg, MsgTypeGetStatusResponse, gsr)
		}

	case MsgTypeHealthCheck:
		m.sendResponse(msg, MsgTypeHealthCheckOk, &proto.BecomeLeaderResponse{})
	default:
		panic(fmt.Sprintf("unknown msgType: %v", msgType))
	}
}

func (m *maelstromGrpcProvider) HandleOxiaStreamRequest(msgType MsgType, msg *Message[OxiaStreamMessage], message pb.Message) {
	slog.Info(
		"HandleOxiaStreamRequest",
		slog.Any("msg-type", msgType),
	)
	switch msgType {
	case MsgTypeAppend:
		key := fmt.Sprintf("%s-%d", msg.Src, msg.Body.StreamId)

		m.Lock()
		stream, alreadyCreated := m.replicateStreams[key]
		if !alreadyCreated {
			stream = newMaelstromReplicateServerStream(msg)
			m.replicateStreams[key] = stream

			go func() {
				err := m.getService(oxiaLogReplication).(proto.OxiaLogReplicationServer).Replicate(stream)
				if err != nil {
					slog.Warn(
						"failed to call replicate",
						slog.Any("error", err),
					)
				}
			}()
		}
		m.Unlock()

		stream.requests <- message.(*proto.Append)
	default:
		slog.Info(
			"HandleOxiaStreamRequest with unsupported message",
			slog.Any("msg-type", msgType),
		)
	}
}

func (m *maelstromGrpcProvider) HandleClientRequest(msgType MsgType, msg any) {
	switch msgType {
	case MsgTypeWrite:
		w := msg.(*Message[Write])
		if res, err := m.getService(oxiaClient).(proto.OxiaClientServer).Write(context.Background(), &proto.WriteRequest{
			Shard: pb.Int64(0),
			Puts: []*proto.PutRequest{{
				Key:               fmt.Sprintf("%d", w.Body.Key),
				Value:             []byte(fmt.Sprintf("%d", w.Body.Value)),
				ExpectedVersionId: nil,
			}},
		}); err != nil {
			sendError(w.Body.MsgId, w.Src, err)
		} else if res.Puts[0].Status != proto.Status_OK {
			sendError(w.Body.MsgId, w.Src, errors.Errorf("Failed to perform write op: %#v", res.Puts[0].Status))
		} else {
			// Ok
			b, _ := json.Marshal(&Message[BaseMessageBody]{
				Src:  thisNode,
				Dest: w.Src,
				Body: BaseMessageBody{
					Type:      MsgTypeWriteOk,
					InReplyTo: &w.Body.MsgId,
				},
			})

			fmt.Fprintln(os.Stdout, string(b))
		}

	case MsgTypeRead:
		r := msg.(*Message[Read])
		stream := newMaelstromReadServerStream()
		err := m.getService(oxiaClient).(proto.OxiaClientServer).Read(&proto.ReadRequest{
			Shard: pb.Int64(0),
			Gets: []*proto.GetRequest{{
				Key:          fmt.Sprintf("%d", r.Body.Key),
				IncludeValue: true,
			}},
		}, stream)
		if err != nil {
			sendError(r.Body.MsgId, r.Src, err)
			return
		}
		res := <-stream.ch
		switch {
		case res.Gets[0].Status == proto.Status_KEY_NOT_FOUND:
			sendErrorWithCode(r.Body.MsgId, r.Src, 20, "key-does-not-exist")
		case res.Gets[0].Status != proto.Status_OK:
			sendError(r.Body.MsgId, r.Src, errors.Errorf("Failed to perform write op: %#v", res.Gets[0].Status))
		default:
			// Ok
			var value int64
			_, _ = fmt.Sscanf(string(res.Gets[0].Value), "%d", &value)
			b, _ := json.Marshal(&Message[ReadResponse]{
				Src:  thisNode,
				Dest: r.Src,
				Body: ReadResponse{
					BaseMessageBody: BaseMessageBody{
						Type:      MsgTypeReadOk,
						InReplyTo: &r.Body.MsgId,
					},
					Value: value,
				},
			})

			fmt.Fprintln(os.Stdout, string(b))
		}

	case MsgTypeCas:
		c := msg.(*Message[Cas])
		stream := newMaelstromReadServerStream()
		err := m.getService(oxiaClient).(proto.OxiaClientServer).Read(&proto.ReadRequest{
			Shard: pb.Int64(0),
			Gets: []*proto.GetRequest{{
				Key:          fmt.Sprintf("%d", c.Body.Key),
				IncludeValue: true,
			}},
		}, stream)
		if err != nil {
			sendError(c.Body.MsgId, c.Src, err)
			return
		}
		res := <-stream.ch
		if res.Gets[0].Status == proto.Status_KEY_NOT_FOUND {
			sendErrorWithCode(c.Body.MsgId, c.Src, 20, "key-does-not-exist")
			return
		} else if res.Gets[0].Status != proto.Status_OK {
			sendError(c.Body.MsgId, c.Src, errors.Errorf("Failed to perform write op: %#v", res.Gets[0].Status))
			return
		}

		// Check the existing value
		var existingValue int64
		_, _ = fmt.Sscanf(string(res.Gets[0].Value), "%d", &existingValue)
		if existingValue != c.Body.From {
			sendErrorWithCode(c.Body.MsgId, c.Src, 22, "precondition-failed")
			return
		}

		// Write it back with conditional write
		if writeRes, err := m.getService(oxiaClient).(proto.OxiaClientServer).Write(context.Background(), &proto.WriteRequest{
			Shard: pb.Int64(0),
			Puts: []*proto.PutRequest{{
				Key:               fmt.Sprintf("%d", c.Body.Key),
				Value:             []byte(fmt.Sprintf("%d", c.Body.To)),
				ExpectedVersionId: pb.Int64(res.Gets[0].Version.VersionId),
			}},
		}); err != nil {
			sendError(c.Body.MsgId, c.Src, err)
		} else if writeRes.Puts[0].Status != proto.Status_OK {
			sendError(c.Body.MsgId, c.Src, errors.Errorf("Failed to perform write op: %#v", writeRes.Puts[0].Status))
		} else {
			// Ok
			b, _ := json.Marshal(&Message[BaseMessageBody]{
				Src:  thisNode,
				Dest: c.Src,
				Body: BaseMessageBody{
					Type:      MsgTypeCasOk,
					InReplyTo: &c.Body.MsgId,
				},
			})

			fmt.Fprintln(os.Stdout, string(b))
		}

	default:
		slog.Error(
			"Unexpected request",
			slog.Any("msg-type", msgType),
		)
		os.Exit(1)
	}
}

func (m *maelstromGrpcProvider) sendResponse(req *Message[OxiaMessage], msgType MsgType, response pb.Message) {
	b, err := json.Marshal(&Message[OxiaMessage]{
		Src:  thisNode,
		Dest: req.Src,
		Body: OxiaMessage{
			BaseMessageBody: BaseMessageBody{
				Type:      msgType,
				MsgId:     msgIdGenerator.Add(1),
				InReplyTo: &req.Body.MsgId,
			},
			OxiaMsg: toJSON(response),
		},
	})
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
}

func (m *maelstromGrpcProvider) getService(name string) any {
	r, ok := m.services[name]
	if !ok {
		slog.Error(
			"Service not found",
			slog.String("service", name),
		)
	}

	return r
}

func newMaelstromReadServerStream() *maelstromReadServerStream {
	return &maelstromReadServerStream{
		ch: make(chan *proto.ReadResponse, 1),
	}
}

type maelstromReadServerStream struct {
	BaseStream
	ch chan *proto.ReadResponse
}

func (m *maelstromReadServerStream) Send(response *proto.ReadResponse) error {
	m.ch <- response
	return nil
}

func (m *maelstromReadServerStream) SetHeader(metadata.MD) error {
	panic("not implemented")
}

func (m *maelstromReadServerStream) SendHeader(metadata.MD) error {
	panic("not implemented")
}

func (m *maelstromReadServerStream) SetTrailer(metadata.MD) {
	panic("not implemented")
}

type maelstromGrpcServer struct {
}

func (m *maelstromGrpcServer) Close() error {
	return nil
}

func (m *maelstromGrpcServer) Port() int {
	return 0
}

type maelstromReplicateServerStream struct {
	BaseStream

	requests chan *proto.Append
	streamId int64
	client   string
}

func (m *maelstromReplicateServerStream) SetHeader(metadata.MD) error {
	panic("implement me")
}

func (m *maelstromReplicateServerStream) Send(response *proto.Ack) error {
	b, _ := json.Marshal(&Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.client,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeAck,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  toJSON(response),
			StreamId: m.streamId,
		},
	})

	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func (m *maelstromReplicateServerStream) Recv() (*proto.Append, error) {
	return <-m.requests, nil
}

func (m *maelstromReplicateServerStream) Context() context.Context {
	md := metadata.New(map[string]string{
		"shard-id":  "0",
		"namespace": "default",
	})
	return metadata.NewIncomingContext(context.Background(), md)
}

func newMaelstromReplicateServerStream(msg *Message[OxiaStreamMessage]) *maelstromReplicateServerStream {
	return &maelstromReplicateServerStream{
		client:   msg.Src,
		streamId: msg.Body.StreamId,
		requests: make(chan *proto.Append),
	}
}
