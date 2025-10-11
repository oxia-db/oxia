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

	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/proto"
)

type maelstromReplicationRpcProvider struct {
	sync.Mutex

	dispatcher Dispatcher

	replicateStreams map[int64]*maelstromReplicateClient
}

func newMaelstromReplicationRpcProvider() *maelstromReplicationRpcProvider {
	return &maelstromReplicationRpcProvider{
		replicateStreams: make(map[int64]*maelstromReplicateClient),
	}
}

func (r *maelstromReplicationRpcProvider) Close() error {
	return nil
}

func (r *maelstromReplicationRpcProvider) GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64, term int64) (
	proto.OxiaLogReplication_ReplicateClient, error) {
	s := &maelstromReplicateClient{
		ctx:       ctx,
		follower:  follower,
		shard:     shard,
		streamId:  msgIdGenerator.Add(1),
		md:        make(metadata.MD),
		responses: make(chan *proto.Ack),
	}

	r.Lock()
	defer r.Unlock()
	r.replicateStreams[s.streamId] = s
	return s, nil
}

func (r *maelstromReplicationRpcProvider) HandleAck(streamId int64, res *proto.Ack) {
	s, ok := r.replicateStreams[streamId]
	if !ok {
		slog.Warn(
			"Stream not found",
			slog.Int64("stream-id", streamId),
		)
		return
	}

	s.responses <- res
}

func (r *maelstromReplicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	res, err := r.dispatcher.RpcRequest(context.Background(), follower, MsgTypeTruncateRequest, req)
	if err != nil {
		return nil, err
	}
	return res.(*proto.TruncateResponse), nil
}

func (r *maelstromReplicationRpcProvider) SendSnapshot(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	panic("not implemented")
}

// //////// ReplicateClient.
type maelstromReplicateClient struct {
	BaseStream

	ctx       context.Context
	follower  string
	shard     int64
	streamId  int64
	md        metadata.MD
	responses chan *proto.Ack
}

func (m *maelstromReplicateClient) Send(request *proto.Append) error {
	b, err := json.Marshal(&Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.follower,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeAppend,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  toJSON(request),
			StreamId: m.streamId,
		},
	})
	if err != nil {
		panic("failed to serialize json")
	}

	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func (m *maelstromReplicateClient) Recv() (*proto.Ack, error) {
	select {
	case r := <-m.responses:
		return r, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *maelstromReplicateClient) Header() (metadata.MD, error) {
	return m.md, nil
}
