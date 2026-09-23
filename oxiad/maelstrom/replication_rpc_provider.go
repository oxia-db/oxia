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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
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
		responses: make(chan *proto.Ack, 1024),
		failed:    make(chan error, 1),
	}

	r.Lock()
	r.replicateStreams[s.streamId] = s
	r.Unlock()

	// When the follower cursor tears the stream down, tell the follower: its
	// Replicate handler would otherwise stay parked on the stream forever
	// (there is no transport to break), which in turn blocks the follower's
	// NewTerm while it waits for the log synchronizer to close.
	context.AfterFunc(ctx, func() {
		r.removeStream(s.streamId)
		sendStreamError(follower, s.streamId)
	})

	return s, nil
}

func (r *maelstromReplicationRpcProvider) removeStream(streamId int64) {
	r.Lock()
	defer r.Unlock()
	delete(r.replicateStreams, streamId)
}

func (r *maelstromReplicationRpcProvider) HandleAck(streamId int64, res *proto.Ack) {
	r.Lock()
	s, ok := r.replicateStreams[streamId]
	r.Unlock()
	if !ok {
		slog.Warn(
			"Stream not found",
			slog.Int64("stream-id", streamId),
		)
		return
	}

	// Never block the dispatcher: acks are cumulative, so one dropped on
	// overflow is covered by the next.
	select {
	case s.responses <- res:
	default:
		slog.Warn(
			"Dropping ack on stream buffer overflow",
			slog.Int64("stream-id", streamId),
		)
	}
}

// HandleStreamError fails the client side of a replicate stream after the
// follower declared it dead: the cursor's Recv() returns an error and the
// cursor reconnects from the last acked offset with a fresh stream.
func (r *maelstromReplicationRpcProvider) HandleStreamError(streamId int64) {
	r.Lock()
	s, ok := r.replicateStreams[streamId]
	delete(r.replicateStreams, streamId)
	r.Unlock()
	if !ok {
		return
	}

	select {
	case s.failed <- status.Error(codes.Unavailable, "replicate stream reset by peer"):
	default:
	}
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
	failed    chan error
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
	case err := <-m.failed:
		return nil, err
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *maelstromReplicateClient) Header() (metadata.MD, error) {
	return m.md, nil
}
