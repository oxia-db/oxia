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

package internal

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{hashRange(1, 2), hashRange(3, 6), false},
		{hashRange(1, 4), hashRange(3, 6), true},
		{hashRange(4, 5), hashRange(3, 6), true},
		{hashRange(5, 8), hashRange(3, 6), true},
		{hashRange(7, 8), hashRange(3, 6), false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}

// Fails right after sending the shard assignments.
type failingAssignmentsStream struct {
	grpc.ClientStream
	namespace string
	sent      bool
}

func (s *failingAssignmentsStream) Recv() (*proto.ShardAssignments, error) {
	if s.sent {
		return nil, status.Error(codes.Unavailable, "connection reset")
	}
	s.sent = true
	return &proto.ShardAssignments{Namespaces: map[string]*proto.NamespaceShardsAssignment{
		s.namespace: {Assignments: []*proto.ShardAssignment{{
			ShardBoundaries: &proto.ShardAssignment_Int32HashRange{Int32HashRange: &proto.Int32HashRange{}},
		}}},
	}}, nil
}

// Serves the shard assignments from getShardAssignments.
type assignmentsClientPool struct {
	rpc.ClientPool
	proto.OxiaClientClient
	getShardAssignments func(ctx context.Context,
		request *proto.ShardAssignmentsRequest) (grpc.ServerStreamingClient[proto.ShardAssignments], error)
}

func (p *assignmentsClientPool) GetClientRpc(string) (proto.OxiaClientClient, error) {
	return p, nil
}

func (p *assignmentsClientPool) GetShardAssignments(ctx context.Context, request *proto.ShardAssignmentsRequest,
	_ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ShardAssignments], error) {
	return p.getShardAssignments(ctx, request)
}

// Signals the warnings, such as the one logged before retrying to receive the
// shard assignments.
type warningsHandler struct {
	slog.Handler
	warnings chan struct{}
}

func (h *warningsHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level == slog.LevelWarn {
		select {
		case h.warnings <- struct{}{}:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

// Closing the shard manager interrupts the wait before it retries to receive
// the assignments. It must stop without reporting a failure: once assignments
// were received, reporting it blocks forever.
func TestShardManagerCloseWhileWaitingToRetry(t *testing.T) {
	logs := &warningsHandler{Handler: slog.Default().Handler(), warnings: make(chan struct{}, 1)}
	s := &shardManagerImpl{
		clientPool: &assignmentsClientPool{getShardAssignments: func(_ context.Context,
			request *proto.ShardAssignmentsRequest) (grpc.ServerStreamingClient[proto.ShardAssignments], error) {
			return &failingAssignmentsStream{namespace: request.Namespace}, nil
		}},
		namespace: "default",
		shards:    make(map[int64]Shard),
		updatedWg: concurrent.NewWaitGroup(1),
		logger:    slog.New(logs),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		s.receiveWithRecovery()
		close(stopped)
	}()

	select {
	case <-logs.warnings:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the failed stream was not retried")
	}

	assert.NoError(t, s.Close())
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the shard manager kept receiving the assignments after it was closed")
	}
}

// A shard manager that does not receive the initial assignments in time fails
// to start, and must stop requesting them.
func TestNewShardManagerStopsReceivingOnTimeout(t *testing.T) {
	canceled := make(chan struct{})
	pool := &assignmentsClientPool{getShardAssignments: func(ctx context.Context,
		_ *proto.ShardAssignmentsRequest) (grpc.ServerStreamingClient[proto.ShardAssignments], error) {
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}}
	_, err := NewShardManager(NewShardStrategy(), pool, "localhost:6648", "default", 100*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the shard manager kept requesting the assignments after it failed to start")
	}
}
