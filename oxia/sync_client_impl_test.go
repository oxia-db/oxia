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

package oxia

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

type neverCompleteAsyncClient struct {
}

func (c *neverCompleteAsyncClient) Close() error { return nil }

func (c *neverCompleteAsyncClient) Put(key string, value []byte, options ...PutOption) <-chan PutResult {
	return make(chan PutResult)
}

func (c *neverCompleteAsyncClient) Delete(key string, options ...DeleteOption) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) Get(key string, options ...GetOption) <-chan GetResult {
	return make(chan GetResult)
}

func (c *neverCompleteAsyncClient) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...ListOption) <-chan ListResult {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) RangeScan(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...RangeScanOption) <-chan GetResult {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) GetNotifications() (Notifications, error) {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) GetSequenceUpdates(ctx context.Context, prefixKey string, options ...GetSequenceUpdatesOption) (<-chan string, error) {
	panic("not implemented")
}

func TestCancelContext(t *testing.T) {
	_asyncClient := &neverCompleteAsyncClient{}
	syncClient := newSyncClient(_asyncClient)

	assertCancellable(t, func(ctx context.Context) error {
		_, _, err := syncClient.Put(ctx, "/a", []byte{})
		return err
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.Delete(ctx, "/a")
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.DeleteRange(ctx, "/a", "/b")
	})
	assertCancellable(t, func(ctx context.Context) error {
		_, _, _, err := syncClient.Get(ctx, "/a")
		return err
	})

	err := syncClient.Close()
	assert.NoError(t, err)
}

// leaderServer leads the only shard of the default namespace, once elected. It
// reports the keys of the writes it receives, and it answers the write of the
// key "stuck" only once released.
type leaderServer struct {
	proto.UnimplementedOxiaClientServer
	address  string
	elected  chan struct{}
	received chan string
	release  chan struct{}
}

func startLeaderServer(t *testing.T) *leaderServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &leaderServer{
		address:  listener.Addr().String(),
		elected:  make(chan struct{}),
		received: make(chan string, 100),
		release:  make(chan struct{}),
	}
	server := grpc.NewServer()
	proto.RegisterOxiaClientServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return s
}

// elect makes the server the leader of the shard in the shard assignments.
func (s *leaderServer) elect() {
	close(s.elected)
}

func (s *leaderServer) GetShardAssignments(_ *proto.ShardAssignmentsRequest,
	stream proto.OxiaClient_GetShardAssignmentsServer) error {
	select {
	case <-s.elected:
	default:
		if err := stream.Send(s.assignments("")); err != nil {
			return err
		}
		select {
		case <-s.elected:
		case <-stream.Context().Done():
			return nil
		}
	}
	if err := stream.Send(s.assignments(s.address)); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (*leaderServer) assignments(leader string) *proto.ShardAssignments {
	return &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: leader,
					ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
						Int32HashRange: &proto.Int32HashRange{MinHashInclusive: 0, MaxHashInclusive: math.MaxUint32},
					},
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
}

func (s *leaderServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		response := &proto.WriteResponse{}
		for _, put := range request.Puts {
			if err := s.receive(stream.Context(), put.Key); err != nil {
				return err
			}
			response.Puts = append(response.Puts, &proto.PutResponse{Version: &proto.Version{}})
		}
		for _, del := range request.Deletes {
			if err := s.receive(stream.Context(), del.Key); err != nil {
				return err
			}
			response.Deletes = append(response.Deletes, &proto.DeleteResponse{})
		}
		for _, deleteRange := range request.DeleteRanges {
			if err := s.receive(stream.Context(), deleteRange.StartInclusive); err != nil {
				return err
			}
			response.DeleteRanges = append(response.DeleteRanges, &proto.DeleteRangeResponse{})
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (s *leaderServer) receive(ctx context.Context, key string) error {
	s.received <- key
	if key != "stuck" {
		return nil
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *leaderServer) nextReceived(t *testing.T) string {
	t.Helper()

	select {
	case key := <-s.received:
		return key
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the leader did not receive a write")
		return ""
	}
}

// syncWrites are the writes of the key "queued", with every write operation of
// the SyncClient.
var syncWrites = []struct {
	name  string
	write func(context.Context, SyncClient) error
}{
	{"put", func(ctx context.Context, client SyncClient) error {
		_, _, err := client.Put(ctx, "queued", []byte("value"))
		return err
	}},
	{"delete", func(ctx context.Context, client SyncClient) error {
		return client.Delete(ctx, "queued")
	}},
	{"delete-range", func(ctx context.Context, client SyncClient) error {
		return client.DeleteRange(ctx, "queued", "queued/")
	}},
}

// assertNotSentWrite writes the key "queued" with a context that ends before
// the write can be sent: it must fail with an error telling that it was not
// sent.
func assertNotSentWrite(t *testing.T, write func(context.Context, SyncClient) error, client SyncClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := write(ctx, client)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, model.IsNotSent(err), "the error must tell that the write was not sent: %v", err)
}

// With a single write in flight per shard, the writes queued behind one that
// the leader does not answer, e.g. because it was deposed, can wait longer than
// the context of their caller. Once the SyncClient reported the failure, the
// write must never be sent: the application can issue it again.
func TestSyncClient_QueuedWriteIsNotSentAfterItsContextEnds(t *testing.T) {
	for _, tt := range syncWrites {
		t.Run(tt.name, func(t *testing.T) {
			leader := startLeaderServer(t)
			leader.elect()
			client, err := NewSyncClient(leader.address)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, client.Close())
			}()

			stuck := make(chan error, 1)
			go func() {
				_, _, err := client.Put(context.Background(), "stuck", []byte("value"))
				stuck <- err
			}()
			assert.Equal(t, "stuck", leader.nextReceived(t))

			assertNotSentWrite(t, tt.write, client)

			close(leader.release)
			assert.NoError(t, <-stuck)

			// The next write is sent after the queued one would have been
			_, _, err = client.Put(context.Background(), "next", []byte("value"))
			assert.NoError(t, err)
			assert.Equal(t, "next", leader.nextReceived(t))
		})
	}
}

// While the shard has no leader, e.g. during an election, the client retries
// a write without sending it. The write must not be sent once the SyncClient
// reported the failure, although its request was already being retried.
func TestSyncClient_WriteIsNotSentAfterItsContextEndsWithoutLeader(t *testing.T) {
	for _, tt := range syncWrites {
		t.Run(tt.name, func(t *testing.T) {
			leader := startLeaderServer(t)
			client, err := NewSyncClient(leader.address)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, client.Close())
			}()

			assertNotSentWrite(t, tt.write, client)

			leader.elect()

			// The next write is sent after the queued one would have been
			_, _, err = client.Put(context.Background(), "next", []byte("value"))
			assert.NoError(t, err)
			assert.Equal(t, "next", leader.nextReceived(t))
		})
	}
}

func assertCancellable(t *testing.T, operationFunc func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error)
	go func() {
		errCh <- operationFunc(ctx)
	}()

	cancel()

	assert.ErrorIs(t, <-errCh, context.Canceled)
}
