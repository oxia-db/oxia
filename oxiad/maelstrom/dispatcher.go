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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
)

type Dispatcher interface {
	ReceivedMessage(msgType MsgType, m any, message pb.Message)
	RpcRequest(ctx context.Context, dest string, msgType MsgType, message pb.Message) (pb.Message, error)
}

type ResponseError struct {
	msg pb.Message
	err error
}

type ProxiedRequest struct {
	msgType MsgType
	msg     any
}

var msgIdGenerator atomic.Int64

type dispatcher struct {
	sync.Mutex
	requests        map[int64]chan ResponseError
	proxiedRequests map[int64]ProxiedRequest

	replicationProvider *maelstromReplicationRpcProvider
	grpcProvider        *maelstromGrpcProvider
	currentLeader       string
}

func newDispatcher(provider *maelstromGrpcProvider, replicationProvider *maelstromReplicationRpcProvider) *dispatcher {
	d := &dispatcher{
		requests:            make(map[int64]chan ResponseError),
		proxiedRequests:     make(map[int64]ProxiedRequest),
		grpcProvider:        provider,
		replicationProvider: replicationProvider,
	}

	d.replicationProvider.dispatcher = d
	return d
}

func (d *dispatcher) onOxiaRequestMessage(msgType MsgType, m *Message[OxiaMessage], message pb.Message) {
	go d.grpcProvider.HandleOxiaRequest(msgType, m, message)
}

func (d *dispatcher) onOxiaResponseMessage(_ MsgType, res *Message[OxiaMessage], message pb.Message) {
	ch, ok := d.requests[*res.Body.InReplyTo]
	if !ok {
		slog.Error("Invalid response id")
		os.Exit(1)
	}

	ch <- ResponseError{msg: message}
}

func (d *dispatcher) onOxiaStreamRequestMessage(msgType MsgType, m any, message pb.Message) {
	switch msgType {
	case MsgTypeShardAssignmentsResponse:
		r := message.(*proto.ShardAssignments)
		d.currentLeader = r.Namespaces[constant.DefaultNamespace].Assignments[0].Leader
		slog.Info(
			"Received notification of new leader",
			slog.String("leader", d.currentLeader),
		)

	case MsgTypeAppend:
		msg := m.(*Message[OxiaStreamMessage])
		go d.grpcProvider.HandleOxiaStreamRequest(msgType, msg, message)

	case MsgTypeAck:
		streamId := m.(*Message[OxiaStreamMessage]).Body.StreamId
		go d.replicationProvider.HandleAck(streamId, message.(*proto.Ack))

	default:
		panic(fmt.Sprintf("Unknown message type: %v", msgType))
	}
}

func (d *dispatcher) onOxiaProxiedMessage(msgType MsgType, m any, _ pb.Message) {
	proxiedMsgId, inReplyTo, _ := getOrigin(msgType, m)

	slog.Info(
		"Received response for proxied message",
		slog.Int64("proxied-msg-id", proxiedMsgId),
		slog.Int64("in-reply-to", inReplyTo),
	)

	switch msgType {
	case MsgTypeHealthCheckOk:
		res := m.(*Message[BaseMessageBody])
		if ch, ok := d.requests[*res.Body.InReplyTo]; !ok {
			slog.Error("Invalid response id")
			os.Exit(1)
		} else { //nolint:revive
			ch <- ResponseError{msg: nil, err: nil}
		}
	case MsgTypeWrite, MsgTypeRead, MsgTypeCas:
		switch d.currentLeader {
		case "":
			sendErrorNotInitialized(msgType, m)
		case thisNode:
			// We should answer
			go d.grpcProvider.HandleClientRequest(msgType, m)
		default:
			d.proxyToLeader(msgType, m)
		}

		// Responses for proxied requests
	case MsgTypeWriteOk, MsgTypeCasOk:
		if pr, ok := d.proxiedRequests[inReplyTo]; ok {
			msgId, _, origin := getOrigin(pr.msgType, pr.msg)
			pm := &Message[BaseMessageBody]{
				Src:  thisNode,
				Dest: origin,
				Body: BaseMessageBody{
					Type:      msgType,
					InReplyTo: &msgId,
				},
			}
			b, _ := json.Marshal(pm)
			fmt.Fprintln(os.Stdout, string(b))
			delete(d.proxiedRequests, msgId)
		}

	case MsgTypeReadOk:
		if pr, ok := d.proxiedRequests[inReplyTo]; ok {
			msgId, _, origin := getOrigin(pr.msgType, pr.msg)
			pm := &Message[ReadResponse]{
				Src:  thisNode,
				Dest: origin,
				Body: ReadResponse{
					BaseMessageBody: BaseMessageBody{
						Type:      MsgTypeReadOk,
						InReplyTo: &msgId,
					},
					Value: m.(*Message[ReadResponse]).Body.Value,
				},
			}
			b, _ := json.Marshal(pm)
			fmt.Fprintln(os.Stdout, string(b))
			delete(d.proxiedRequests, msgId)
		}

	case MsgTypeError:
		e := m.(*Message[ErrorResponse])
		if ch, ok := d.requests[inReplyTo]; ok {
			ch <- ResponseError{
				msg: nil,
				err: errors.New(e.Body.Text),
			}

			delete(d.requests, inReplyTo)
		} else if pr, ok := d.proxiedRequests[inReplyTo]; ok {
			// Got error for a proxied request
			msgId, _, origin := getOrigin(pr.msgType, pr.msg)
			sendErrorWithCode(msgId, origin, e.Body.Code, e.Body.Text)
			delete(d.proxiedRequests, inReplyTo)
		}
	default:
		panic(fmt.Sprintf("Unknown message type: %v", msgType))
	}
}

func (d *dispatcher) ReceivedMessage(msgType MsgType, m any, message pb.Message) {
	d.Lock()
	defer d.Unlock()

	slog.Info(
		"RECEIVED MESSAGE",
		slog.Any("msg", m),
		slog.Any("msg-type", msgType),
	)

	switch {
	case msgType.isOxiaRequest():
		d.onOxiaRequestMessage(msgType, m.(*Message[OxiaMessage]), message)
	case msgType.isOxiaResponse():
		d.onOxiaResponseMessage(msgType, m.(*Message[OxiaMessage]), message)
	case msgType.isOxiaStreamRequest():
		d.onOxiaStreamRequestMessage(msgType, m, message)
	default:
		d.onOxiaProxiedMessage(msgType, m, message)
	}
}

func getOrigin(msgType MsgType, m any) (msgId int64, inReplyTo int64, origin string) {
	switch msgType {
	case MsgTypeWrite:
		w := m.(*Message[Write])
		origin = w.Src
		msgId = w.Body.MsgId
		if w.Body.InReplyTo != nil {
			inReplyTo = *w.Body.InReplyTo
		}

	case MsgTypeRead:
		r := m.(*Message[Read])
		origin = r.Src
		msgId = r.Body.MsgId
		if r.Body.InReplyTo != nil {
			inReplyTo = *r.Body.InReplyTo
		}
	case MsgTypeCas:
		c := m.(*Message[Cas])
		origin = c.Src
		msgId = c.Body.MsgId
		if c.Body.InReplyTo != nil {
			inReplyTo = *c.Body.InReplyTo
		}

	case MsgTypeWriteOk, MsgTypeCasOk:
		w := m.(*Message[BaseMessageBody])
		origin = w.Src
		msgId = w.Body.MsgId
		if w.Body.InReplyTo != nil {
			inReplyTo = *w.Body.InReplyTo
		}

	case MsgTypeReadOk:
		r := m.(*Message[ReadResponse])
		origin = r.Src
		msgId = r.Body.MsgId
		if r.Body.InReplyTo != nil {
			inReplyTo = *r.Body.InReplyTo
		}
	case MsgTypeError:
		e := m.(*Message[ErrorResponse])
		origin = e.Src
		msgId = e.Body.MsgId
		if e.Body.InReplyTo != nil {
			inReplyTo = *e.Body.InReplyTo
		}
	default:
		panic(fmt.Sprintf("Unknown message type: %v", msgType))
	}

	return msgId, inReplyTo, origin
}

func sendErrorNotInitialized(msgType MsgType, m any) {
	msgId, _, origin := getOrigin(msgType, m)

	sendErrorWithCode(msgId, origin, 11, "temporarily-unavailable")
}

func (d *dispatcher) proxyToLeader(msgType MsgType, m any) {
	proxyMsgId := msgIdGenerator.Add(1)

	slog.Info(
		"Added new proxy msg id",
		slog.Int64("proxied-msg-id", proxyMsgId),
		slog.Any("msg-type", msgType),
	)
	d.proxiedRequests[proxyMsgId] = ProxiedRequest{
		msgType: msgType,
		msg:     m,
	}

	var pm any
	switch msgType {
	case MsgTypeWrite:
		w := m.(*Message[Write])
		pm = &Message[Write]{
			Src:  thisNode,
			Dest: d.currentLeader,
			Body: Write{
				BaseMessageBody: BaseMessageBody{
					Type:  MsgTypeWrite,
					MsgId: proxyMsgId,
				},
				Key:   w.Body.Key,
				Value: w.Body.Value,
			},
		}
	case MsgTypeRead:
		r := m.(*Message[Read])
		pm = &Message[Read]{
			Src:  thisNode,
			Dest: d.currentLeader,
			Body: Read{
				BaseMessageBody: BaseMessageBody{
					Type:  MsgTypeRead,
					MsgId: proxyMsgId,
				},
				Key: r.Body.Key,
			},
		}
	case MsgTypeCas:
		cas := m.(*Message[Cas])
		pm = &Message[Cas]{
			Src:  thisNode,
			Dest: d.currentLeader,
			Body: Cas{
				BaseMessageBody: BaseMessageBody{
					Type:  MsgTypeCas,
					MsgId: proxyMsgId,
				},
				Key:  cas.Body.Key,
				From: cas.Body.From,
				To:   cas.Body.To,
			},
		}
	default:
		panic(fmt.Sprintf("unknown comparison type: %v", msgType))
	}

	b, _ := json.Marshal(pm)
	fmt.Fprintln(os.Stdout, string(b))
}

func (d *dispatcher) RpcRequest(ctx context.Context, dest string, msgType MsgType, message pb.Message) (pb.Message, error) {
	msgId := msgIdGenerator.Add(1)

	d.Lock()
	ch := make(chan ResponseError, 1)
	d.requests[msgId] = ch
	d.Unlock()

	req := &Message[OxiaMessage]{
		Src:  thisNode,
		Dest: dest,
		Body: OxiaMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  msgType,
				MsgId: msgId,
			},
			OxiaMsg: toJSON(message),
		},
	}

	b, err := json.Marshal(req)
	if err != nil {
		slog.Error(
			"failed to serialize json",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	slog.Info(
		"Sending message",
		slog.Any("msg", b),
	)

	fmt.Fprintln(os.Stdout, string(b))

	select {
	case w := <-ch:
		return w.msg, w.err
	case <-time.After(5 * time.Second):
		return nil, errors.New("timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

var protoMarshal = protojson.MarshalOptions{
	EmitUnpopulated: true,
}

func toJSON(message pb.Message) []byte {
	r, err := protoMarshal.Marshal(message)
	if err != nil {
		slog.Error(
			"failed to serialize proto to json",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	return r
}
