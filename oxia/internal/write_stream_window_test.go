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

package internal

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// heldWriteServer receives the write requests of a stream and holds their
// responses until the test answers them, so that several are in flight.
type heldWriteServer struct {
	proto.UnimplementedOxiaClientServer

	mu       sync.Mutex
	received []string
	streams  int
	// answer sends the response of the oldest request held, or ends the
	// stream with the error
	answer chan error
}

func newHeldWriteServer() *heldWriteServer {
	return &heldWriteServer{answer: make(chan error)}
}

func (s *heldWriteServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	s.mu.Lock()
	s.streams++
	s.mu.Unlock()

	requests := make(chan *proto.WriteRequest, 100)
	go func() {
		defer close(requests)
		for {
			request, err := stream.Recv()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.received = append(s.received, request.Puts[0].Key)
			s.mu.Unlock()
			requests <- request
		}
	}()

	for {
		select {
		case err := <-s.answer:
			if err != nil {
				return err
			}
			request, ok := <-requests
			if !ok {
				return io.EOF
			}
			response := &proto.WriteResponse{}
			for range request.Puts {
				response.Puts = append(response.Puts, &proto.PutResponse{})
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *heldWriteServer) receivedKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

func newHeldWriteProvider(t *testing.T, server *heldWriteServer) RpcProvider {
	t.Helper()

	return newHeldWriteProviderAt(t, startTestOxiaClientServer(t, server), &testShardManager{})
}

// newHeldWriteProviderAt returns a provider whose shards are led by address.
func newHeldWriteProviderAt(t *testing.T, address string, shardManager *testShardManager) RpcProvider {
	t.Helper()

	shardManager.leader.Store(&address)
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, address,
		func() ShardManager { return shardManager })
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	return provider
}

func putRequest(shardId *int64, key string) *proto.WriteRequest {
	return &proto.WriteRequest{Shard: shardId, Puts: []*proto.PutRequest{{Key: key, Value: []byte(key)}}}
}

// Writes sent asynchronously on a shard reach the leader without waiting for
// the responses of the writes sent before them, in the order they were sent.
func TestRpcProvider_AsyncWritesAreSentBeforeTheirResponses(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "a"))
	second := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "b"))

	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b"}, server.receivedKeys())

	server.answer <- nil
	server.answer <- nil
	_, err := first()
	require.NoError(t, err)
	_, err = second()
	require.NoError(t, err)
}

// A write whose response does not arrive before its deadline closes the
// stream: the writes in flight behind it fail at once instead of each waiting
// for its own deadline, and the next write is sent on a new stream.
func TestRpcProvider_AsyncWriteDeadlineClosesTheStream(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	short, cancelShort := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelShort()
	long, cancelLong := context.WithTimeout(t.Context(), time.Minute)
	defer cancelLong()
	first := provider.ExecuteWriteAsync(short, putRequest(&shardId, "a"))
	second := provider.ExecuteWriteAsync(long, putRequest(&shardId, "b"))
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	_, err := first()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	failed := make(chan error, 1)
	go func() {
		_, err := second()
		failed <- err
	}()
	select {
	case err := <-failed:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the write behind the expired one kept waiting on the stream")
	}

	third := provider.ExecuteWriteAsync(long, putRequest(&shardId, "c"))
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	server.answer <- nil
	_, err = third()
	require.NoError(t, err)
}

// A leader that rejects the writes of a stream, e.g. because it is no longer
// the leader, applied none of the writes it rejected: the writes in flight are
// sent again, in order, to the leader it hints, and the writes issued since
// follow them.
func TestRpcProvider_AsyncWritesRejectedByTheLeaderAreSentToTheNewOne(t *testing.T) {
	newLeader := newHeldWriteServer()
	newLeaderAddress := startTestOxiaClientServer(t, newLeader)
	oldLeader := newHeldWriteServer()
	oldLeaderAddress := startTestOxiaClientServer(t, oldLeader)

	shardManager := &testShardManager{}
	shardManager.leader.Store(&oldLeaderAddress)
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, oldLeaderAddress,
		func() ShardManager { return shardManager })
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "a"))
	second := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "b"))
	require.Eventually(t, func() bool { return len(oldLeader.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	oldLeader.answer <- constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader,
		constant.WithLeaderHint(shardId, newLeaderAddress))
	third := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "c"))

	require.Eventually(t, func() bool { return len(newLeader.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b", "c"}, newLeader.receivedKeys())
	for range 3 {
		newLeader.answer <- nil
	}
	for _, wait := range []func() (*proto.WriteResponse, error){first, second, third} {
		_, err := wait()
		require.NoError(t, err)
	}
}

// rejectingWriteServer rejects every write stream as a node that is not the
// leader, without a hint, and counts the streams.
type rejectingWriteServer struct {
	proto.UnimplementedOxiaClientServer
	streams atomic.Int32
}

func (s *rejectingWriteServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	s.streams.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader)
}

// removableShardManager is a testShardManager whose shard can be removed, as
// after a split.
type removableShardManager struct {
	testShardManager
	removed atomic.Bool
}

func (m *removableShardManager) Exists(int64) bool { return !m.removed.Load() }

// A leader that keeps rejecting the writes in flight gets them again with a
// growing delay, not in a busy loop.
func TestRpcProvider_AsyncWritesRejectedAgainAreResentWithABackoff(t *testing.T) {
	server := &rejectingWriteServer{}
	provider := newHeldWriteProviderAt(t, startTestOxiaClientServer(t, server), &testShardManager{})
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "a"))()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, server.streams.Load(), int32(10), "the writes were sent again without a delay")
}

// When the shard of the writes that its leader rejected is gone, e.g. after a
// split, the writes fail with ErrShardNotFound, so that they are rerouted.
func TestRpcProvider_AsyncWritesRejectedByAGoneShardFail(t *testing.T) {
	server := &rejectingWriteServer{}
	shardManager := &removableShardManager{}
	provider := newHeldWriteProviderAt(t, startTestOxiaClientServer(t, server), &shardManager.testShardManager)
	provider.(*rpcProvider).shardManagerSupplier = func() ShardManager { return shardManager }
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	wait := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "a"))
	require.Eventually(t, func() bool { return server.streams.Load() > 0 }, 10*time.Second, time.Millisecond)
	shardManager.removed.Store(true)

	_, err := wait()
	require.ErrorIs(t, err, constant.ErrShardNotFound)
}

// When the stream breaks, every write in flight on it fails, in order, and is
// never sent again: the leader may have applied it already. The next write is
// sent on a new stream.
func TestRpcProvider_AsyncWritesInFlightFailWhenTheStreamBreaks(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "a"))
	second := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "b"))
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	broken := status.Error(codes.Internal, "stream broken")
	server.answer <- broken
	_, err := first()
	require.Equal(t, codes.Internal, status.Code(err))
	_, err = second()
	require.Equal(t, codes.Internal, status.Code(err))

	third := provider.ExecuteWriteAsync(ctx, putRequest(&shardId, "c"))
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	server.answer <- nil
	_, err = third()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, server.receivedKeys(), "no write was sent again")
}
