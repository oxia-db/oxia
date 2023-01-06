package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"
	"os"
	"oxia/common/container"
	"oxia/proto"
	"sync"
)

const (
	oxiaControl        = "coordination.OxiaControl"
	oxiaLogReplication = "coordination.OxiaLogReplication"
	oxiaClient         = "io.streamnative.oxia.proto.OxiaClient"
)

type maelstromGrpcProvider struct {
	sync.Mutex
	services map[string]any

	addEntriesStreams map[string]*maelstromAddEntriesServerStream
}

func newMaelstromGrpcProvider() *maelstromGrpcProvider {
	return &maelstromGrpcProvider{
		services:          make(map[string]any),
		addEntriesStreams: make(map[string]*maelstromAddEntriesServerStream),
	}
}

func (m *maelstromGrpcProvider) StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar)) (container.GrpcServer, error) {
	log.Info().
		Str("name", name).
		Msg("Start Grpc server")

	registerFunc(m)
	return &maelstromGrpcServer{}, nil
}

func (m *maelstromGrpcProvider) RegisterService(desc *grpc.ServiceDesc, impl any) {
	log.Info().
		Str("service-name", desc.ServiceName).
		Msg("RegisterService")
	m.services[desc.ServiceName] = impl
}

func (m *maelstromGrpcProvider) HandleOxiaRequest(msgType MsgType, msg *Message[OxiaMessage], message pb.Message) {
	switch msgType {
	case MsgTypeFenceRequest:
		if fr, err := m.getService(oxiaControl).(proto.OxiaControlServer).Fence(context.Background(), message.(*proto.FenceRequest)); err != nil {
			sendError(msg.Body.MsgId, msg.Src, err)
		} else {
			m.sendResponse(msg, MsgTypeFenceResponse, fr)
		}

	case MsgTypeBecomeLeaderRequest:
		if blr, err := m.getService(oxiaControl).(proto.OxiaControlServer).BecomeLeader(context.Background(), message.(*proto.BecomeLeaderRequest)); err != nil {
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
		if gsr, err := m.getService(oxiaControl).(proto.OxiaControlServer).GetStatus(context.Background(), message.(*proto.GetStatusRequest)); err != nil {
			sendError(msg.Body.MsgId, msg.Src, err)
		} else {
			m.sendResponse(msg, MsgTypeGetStatusResponse, gsr)
		}

	case MsgTypeHealthCheck:
		m.sendResponse(msg, MsgTypeHealthCheckOk, &proto.BecomeLeaderResponse{})
	}
}

func (m *maelstromGrpcProvider) HandleOxiaStreamRequest(msgType MsgType, msg *Message[OxiaStreamMessage], message pb.Message) {
	log.Info().Interface("msg-type", msgType).Msg("HandleOxiaStreamRequest")
	switch msgType {
	case MsgTypeAddEntryRequest:
		key := fmt.Sprintf("%s-%d", msg.Src, msg.Body.StreamId)

		m.Lock()
		stream, alreadyCreated := m.addEntriesStreams[key]
		if !alreadyCreated {
			stream = newMaelstromAddEntriesServerStream(msg)
			m.addEntriesStreams[key] = stream

			go func() {
				err := m.getService(oxiaLogReplication).(proto.OxiaLogReplicationServer).AddEntries(stream)
				if err != nil {
					log.Warn().Err(err).Msg("failed to call addEntries")
				}
			}()
		}
		m.Unlock()

		stream.requests <- message.(*proto.AddEntryRequest)
	}
}

func (m *maelstromGrpcProvider) HandleClientRequest(msgType MsgType, msg any) {
	switch msgType {
	case MsgTypeWrite:
		w := msg.(*Message[Write])
		if res, err := m.getService(oxiaClient).(proto.OxiaClientServer).Write(context.Background(), &proto.WriteRequest{
			ShardId: pb.Uint32(0),
			Puts: []*proto.PutRequest{{
				Key:             fmt.Sprintf("%d", w.Body.Key),
				Payload:         []byte(fmt.Sprintf("%d", w.Body.Value)),
				ExpectedVersion: nil,
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
		if res, err := m.getService(oxiaClient).(proto.OxiaClientServer).Read(context.Background(), &proto.ReadRequest{
			ShardId: pb.Uint32(0),
			Gets: []*proto.GetRequest{{
				Key:            fmt.Sprintf("%d", r.Body.Key),
				IncludePayload: true,
			}},
		}); err != nil {
			sendError(r.Body.MsgId, r.Src, err)
		} else if res.Gets[0].Status == proto.Status_KEY_NOT_FOUND {
			sendErrorWithCode(r.Body.MsgId, r.Src, 20, "key-does-not-exist")
		} else if res.Gets[0].Status != proto.Status_OK {
			sendError(r.Body.MsgId, r.Src, errors.Errorf("Failed to perform write op: %#v", res.Gets[0].Status))
		} else {
			// Ok
			var value int64
			_, _ = fmt.Sscanf(string(res.Gets[0].Payload), "%d", &value)
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

		res, err := m.getService(oxiaClient).(proto.OxiaClientServer).Read(context.Background(), &proto.ReadRequest{
			ShardId: pb.Uint32(0),
			Gets: []*proto.GetRequest{{
				Key:            fmt.Sprintf("%d", c.Body.Key),
				IncludePayload: true,
			}},
		})
		if err != nil {
			sendError(c.Body.MsgId, c.Src, err)
			return
		} else if res.Gets[0].Status == proto.Status_KEY_NOT_FOUND {
			sendErrorWithCode(c.Body.MsgId, c.Src, 20, "key-does-not-exist")
			return
		} else if res.Gets[0].Status != proto.Status_OK {
			sendError(c.Body.MsgId, c.Src, errors.Errorf("Failed to perform write op: %#v", res.Gets[0].Status))
			return
		}

		// Check the existing value
		var existingValue int64
		_, _ = fmt.Sscanf(string(res.Gets[0].Payload), "%d", &existingValue)
		if existingValue != c.Body.From {
			sendErrorWithCode(c.Body.MsgId, c.Src, 22, "precondition-failed")
			return
		}

		// Write it back with conditional write
		if writeRes, err := m.getService(oxiaClient).(proto.OxiaClientServer).Write(context.Background(), &proto.WriteRequest{
			ShardId: pb.Uint32(0),
			Puts: []*proto.PutRequest{{
				Key:             fmt.Sprintf("%d", c.Body.Key),
				Payload:         []byte(fmt.Sprintf("%d", c.Body.To)),
				ExpectedVersion: pb.Int64(res.Gets[0].Stat.Version),
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
		log.Fatal().Interface("msg-type", msgType).Msg("Unexpected request")
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
			OxiaMsg: toJson(response),
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
		log.Fatal().Str("service", name).Msg("Service not found")
	}

	return r
}

type maelstromGrpcServer struct {
}

func (m *maelstromGrpcServer) Close() error {
	return nil
}

func (m *maelstromGrpcServer) Port() int {
	return 0
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type maelstromAddEntriesServerStream struct {
	BaseStream

	requests chan *proto.AddEntryRequest
	streamId int64
	client   string
}

func (m *maelstromAddEntriesServerStream) SetHeader(md metadata.MD) error {
	panic("implement me")
}

func (m *maelstromAddEntriesServerStream) Send(response *proto.AddEntryResponse) error {
	b, _ := json.Marshal(&Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.client,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeAddEntryResponse,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  toJson(response),
			StreamId: m.streamId,
		},
	})

	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func (m *maelstromAddEntriesServerStream) Recv() (*proto.AddEntryRequest, error) {
	return <-m.requests, nil
}

func (m *maelstromAddEntriesServerStream) Context() context.Context {
	md := metadata.New(map[string]string{"shard-id": "0"})
	return metadata.NewIncomingContext(context.Background(), md)
}

func newMaelstromAddEntriesServerStream(msg *Message[OxiaStreamMessage]) *maelstromAddEntriesServerStream {
	return &maelstromAddEntriesServerStream{
		client:   msg.Src,
		streamId: msg.Body.StreamId,
		requests: make(chan *proto.AddEntryRequest),
	}
}
