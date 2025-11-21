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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/proto"
)

type MsgType string

const (
	MsgTypeInit  MsgType = "init"
	MsgTypeError MsgType = "error"

	// Maelstrom "lin-kv" workload messages.

	MsgTypeWrite   MsgType = "write"
	MsgTypeWriteOk MsgType = "write_ok"
	MsgTypeRead    MsgType = "read"
	MsgTypeReadOk  MsgType = "read_ok"
	MsgTypeCas     MsgType = "cas"
	MsgTypeCasOk   MsgType = "cas_ok"

	/* Oxia specific messages. */

	MsgTypeNewTermRequest       MsgType = "term-req"
	MsgTypeNewTermResponse      MsgType = "term-resp"
	MsgTypeTruncateRequest      MsgType = "truncate-req"
	MsgTypeTruncateResponse     MsgType = "truncate-resp"
	MsgTypeBecomeLeaderRequest  MsgType = "leader-req"
	MsgTypeBecomeLeaderResponse MsgType = "leader-resp"
	MsgTypeAppend               MsgType = "add-entry"
	MsgTypeAck                  MsgType = "ack"
	MsgTypeAddFollowerRequest   MsgType = "add-follower-req"
	MsgTypeAddFollowerResponse  MsgType = "add-follower-resp"
	MsgTypeGetStatusRequest     MsgType = "get-status"
	MsgTypeDeleteShardRequest   MsgType = "delete-shard-req"
	MsgTypeDeleteShardResponse  MsgType = "delete-shard-resp"
	MsgTypeGetStatusResponse    MsgType = "status"
	MsgTypeHealthCheck          MsgType = "health"
	MsgTypeHealthCheckOk        MsgType = "health-ok"

	MsgTypeShardAssignmentsResponse MsgType = "shards"
)

var (
	oxiaRequests = map[MsgType]bool{
		MsgTypeNewTermRequest:      true,
		MsgTypeTruncateRequest:     true,
		MsgTypeBecomeLeaderRequest: true,
		MsgTypeAddFollowerRequest:  true,
		MsgTypeHealthCheck:         true,
		MsgTypeGetStatusRequest:    true,
		MsgTypeDeleteShardRequest:  true,
	}

	oxiaResponses = map[MsgType]bool{
		MsgTypeNewTermResponse:      true,
		MsgTypeTruncateResponse:     true,
		MsgTypeBecomeLeaderResponse: true,
		MsgTypeAddFollowerResponse:  true,
		MsgTypeHealthCheckOk:        true,
		MsgTypeGetStatusResponse:    true,
		MsgTypeDeleteShardResponse:  true,
	}

	oxiaStreamRequests = map[MsgType]bool{
		MsgTypeAppend:                   true,
		MsgTypeAck:                      true,
		MsgTypeShardAssignmentsResponse: true,
	}
)

func (m MsgType) isOxiaRequest() bool {
	r, ok := oxiaRequests[m]
	return ok && r
}

func (m MsgType) isOxiaStreamRequest() bool {
	r, ok := oxiaStreamRequests[m]
	return ok && r
}

func (m MsgType) isOxiaResponse() bool {
	r, ok := oxiaResponses[m]
	return ok && r
}

type Message[Body any] struct {
	Id   int64  `json:"id"`
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body Body   `json:"body"`
}

type BaseMessageBody struct {
	Type      MsgType `json:"type"`
	MsgId     int64   `json:"msg_id"`
	InReplyTo *int64  `json:"in_reply_to"`
}

type Init struct {
	BaseMessageBody
	NodeId   string   `json:"node_id"`
	NodesIDs []string `json:"node_ids"`
}

type Read struct {
	BaseMessageBody
	Key int64 `json:"key"`
}

type Write struct {
	BaseMessageBody
	Key   int64 `json:"key"`
	Value int64 `json:"value"`
}

type Cas struct {
	BaseMessageBody
	Key  int64 `json:"key"`
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

type OxiaMessage struct {
	BaseMessageBody
	OxiaMsg json.RawMessage `json:"m"`
}

type OxiaStreamMessage struct {
	BaseMessageBody
	OxiaMsg  json.RawMessage `json:"m"`
	StreamId int64           `json:"stream_id"`
}

type EmptyResponse struct {
	BaseMessageBody
}

type ReadResponse struct {
	BaseMessageBody
	Value int64 `json:"value"`
}

type ErrorResponse struct {
	BaseMessageBody
	Code int    `json:"code"`
	Text string `json:"text"`
}

func sendResponse(response any) {
	b, err := json.Marshal(response)
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
}

func sendError(msgId int64, dest string, err error) {
	sendErrorWithCode(msgId, dest, 1000, err.Error())
}

func sendErrorWithCode(msgId int64, dest string, code int, text string) {
	b, err := json.Marshal(&Message[ErrorResponse]{
		Src:  thisNode,
		Dest: dest,
		Body: ErrorResponse{
			BaseMessageBody: BaseMessageBody{
				Type:      MsgTypeError,
				MsgId:     msgIdGenerator.Add(1),
				InReplyTo: &msgId,
			},
			Code: code,
			Text: text,
		},
	})
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
}

var jsonMsgMapping = map[MsgType]any{
	MsgTypeInit:    &Message[Init]{},
	MsgTypeWrite:   &Message[Write]{},
	MsgTypeRead:    &Message[Read]{},
	MsgTypeCas:     &Message[Cas]{},
	MsgTypeWriteOk: &Message[BaseMessageBody]{},
	MsgTypeReadOk:  &Message[ReadResponse]{},
	MsgTypeCasOk:   &Message[BaseMessageBody]{},
	MsgTypeError:   &Message[ErrorResponse]{},

	MsgTypeHealthCheck:   &Message[OxiaMessage]{},
	MsgTypeHealthCheckOk: &Message[OxiaMessage]{},

	MsgTypeAppend: &Message[OxiaStreamMessage]{},
}

var protoMsgMapping = map[MsgType]pb.Message{
	MsgTypeNewTermRequest:       &proto.NewTermRequest{},
	MsgTypeNewTermResponse:      &proto.NewTermResponse{},
	MsgTypeTruncateRequest:      &proto.TruncateRequest{},
	MsgTypeTruncateResponse:     &proto.TruncateResponse{},
	MsgTypeBecomeLeaderRequest:  &proto.BecomeLeaderRequest{},
	MsgTypeBecomeLeaderResponse: &proto.BecomeLeaderResponse{},
	MsgTypeAppend:               &proto.Append{},
	MsgTypeAck:                  &proto.Ack{},
	MsgTypeAddFollowerRequest:   &proto.AddFollowerRequest{},
	MsgTypeAddFollowerResponse:  &proto.AddFollowerResponse{},
	MsgTypeGetStatusRequest:     &proto.GetStatusRequest{},
	MsgTypeGetStatusResponse:    &proto.GetStatusResponse{},

	MsgTypeShardAssignmentsResponse: &proto.ShardAssignments{},
}

func parseRequest(line string) (msgType MsgType, msg any, protoMsg pb.Message) {
	frame := &Message[BaseMessageBody]{}

	if err := json.Unmarshal([]byte(line), frame); err != nil {
		slog.Error(
			"failed to unmarshal",
			slog.String("line", line),
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	msg = frame
	msgType = frame.Body.Type
	sm, ok := jsonMsgMapping[frame.Body.Type]
	if ok {
		// Deserialize json again with specific type
		// Deserialize again with the proper struct
		if err := json.Unmarshal([]byte(line), sm); err != nil {
			slog.Error(
				"failed to unmarshal the proper struct",
				slog.Any("error", err),
			)
			os.Exit(1)
		}

		msg = sm
	}

	protoMsg, ok = protoMsgMapping[frame.Body.Type]
	if ok {
		if msgType.isOxiaStreamRequest() {
			om := &Message[OxiaStreamMessage]{}
			if err := json.Unmarshal([]byte(line), om); err != nil {
				slog.Error(
					"failed to unmarshal the proper struct",
					slog.Any("error", err),
				)
				os.Exit(1)
			}

			msg = om
			if err := protojson.Unmarshal(om.Body.OxiaMsg, protoMsg); err != nil {
				slog.Error(
					"failed to unmarshal proto json",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
		} else {
			om := &Message[OxiaMessage]{}
			if err := json.Unmarshal([]byte(line), om); err != nil {
				slog.Error(
					"failed to unmarshal the proper struct",
					slog.Any("error", err),
				)
				os.Exit(1)
			}

			msg = om
			if err := protojson.Unmarshal(om.Body.OxiaMsg, protoMsg); err != nil {
				slog.Error(
					"failed to unmarshal proto json",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
		}
	}

	slog.Info(
		"received message",
		slog.Any("type", msgType),
		slog.Any("proto-msg", protoMsg),
	)
	return msgType, msg, protoMsg
}
