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

package assignment

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

func TestUninitializedAssignmentDispatcher(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
	mockClient := rpc.NewMockShardAssignmentClientStream()
	assert.False(t, dispatcher.Initialized())
	req := &proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace}
	err := dispatcher.RegisterForUpdates(req, mockClient)
	assert.ErrorIs(t, err, constant.ErrNotInitialized)
	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_StandaloneRegisterWithoutAuthority(t *testing.T) {
	dispatcher := NewStandaloneShardAssignmentDispatcher(1, proto.KeySortingType_UNKNOWN)

	// The standalone dispatcher sends the client's own authority as the leader,
	// so it rejects a client stream that carries no authority
	err := dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{}, rpc.NewMockShardAssignmentClientStream())
	assert.Equal(t, codes.Internal, status.Code(err))

	// The rejected client must not keep the lock or stay registered
	require.Eventually(t, dispatcher.Initialized, 10*time.Second, 10*time.Millisecond,
		"the dispatcher lock is still held")
	s := dispatcher.(*shardAssignmentDispatcher)
	s.RLock()
	assert.Empty(t, s.clients)
	s.RUnlock()

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_Initialized(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	assert.False(t, dispatcher.Initialized())
	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1", 0, 100),
					newShardAssignment(1, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	})
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)
	mockClient := rpc.NewMockShardAssignmentClientStream()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		req := &proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
	})

	mockClient.Cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_ReadinessProbe(t *testing.T) {
	healthServer := oxiadcommonrpc.NewClosableHealthServer(t.Context())
	dispatcher := NewShardAssignmentDispatcher(healthServer)
	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	assert.False(t, dispatcher.Initialized())

	// Readiness probe should fail while not initialized
	resp, err := healthServer.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: oxiadcommonrpc.ReadinessProbeService,
	})

	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Nil(t, resp)

	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1", 0, 100),
					newShardAssignment(1, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	})
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	resp, err = healthServer.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: oxiadcommonrpc.ReadinessProbeService,
	})
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_AddClient(t *testing.T) {
	shard0InitialAssignment := newShardAssignment(0, "server1", 0, 100)
	shard1InitialAssignment := newShardAssignment(1, "server2", 100, math.MaxUint32)
	shard1UpdatedAssignment := newShardAssignment(1, "server3", 100, math.MaxUint32)

	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))

	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					shard0InitialAssignment,
					shard1InitialAssignment,
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
	coordinatorStream.AddRequest(request)
	// Wait for the dispatcher to process the initializing request
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	// Should get the whole assignment as they arrived from controller
	mockClient := rpc.NewMockShardAssignmentClientStream()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		req := &proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
	})

	response := mockClient.GetResponse()
	assert.True(t, pb.Equal(request, response))

	request = &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					shard0InitialAssignment,
					shard1UpdatedAssignment,
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
	coordinatorStream.AddRequest(request)

	// Should get the assignment update as they arrived from controller
	response = mockClient.GetResponse()
	assert.True(t, pb.Equal(request, response))

	mockClient.Cancel()
	wg.Wait()

	// Should get the whole assignment with the update applied
	mockClient = rpc.NewMockShardAssignmentClientStream()
	wg2 := sync.WaitGroup{}
	wg2.Add(1)

	go func() {
		req := &proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
		wg2.Done()
	}()

	response = mockClient.GetResponse()
	assert.True(t, pb.Equal(request, response))

	mockClient.Cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

// A data server that lost the stream of the coordinator doesn't hear about the changes of the leaders, so it must not
// give its assignments to the clients: it ends the streams of the ones it has, and refuses the new ones, until the
// coordinator pushes the assignments again. The clients then ask another data server.
func TestShardAssignmentDispatcherRegisterForUpdatesFailsWhenCoordinatorStreamEnds(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
	t.Cleanup(func() { assert.NoError(t, dispatcher.Close()) })

	assignments := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments:    []*proto.ShardAssignment{newShardAssignment(0, "server1", 0, math.MaxUint32)},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
	push := func() *endableCoordinatorStream {
		stream := newEndableCoordinatorStream()
		go func() { _ = dispatcher.PushShardAssignments(stream) }()
		stream.AddRequest(assignments)
		return stream
	}
	request := &proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace}

	coordinator := push()
	require.Eventually(t, dispatcher.Initialized, 10*time.Second, 10*time.Millisecond)

	client := rpc.NewMockShardAssignmentClientStream()
	registered := make(chan error, 1)
	go func() { registered <- dispatcher.RegisterForUpdates(request, client) }()
	assert.True(t, pb.Equal(assignments, client.GetResponse()))

	// The coordinator is lost: the stream of the client ends with a status that it retries elsewhere
	coordinator.end()
	select {
	case err := <-registered:
		assert.ErrorIs(t, err, constant.ErrNotInitialized)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the stream of the client was not ended")
	}
	assert.ErrorIs(t, dispatcher.RegisterForUpdates(request, rpc.NewMockShardAssignmentClientStream()), constant.ErrNotInitialized)

	// The coordinator pushes again: the data server serves the clients
	push()
	require.Eventually(t, func() bool {
		d := dispatcher.(*shardAssignmentDispatcher)
		d.RLock()
		defer d.RUnlock()
		return !d.coordinatorLost
	}, 10*time.Second, 10*time.Millisecond)
	client = rpc.NewMockShardAssignmentClientStream()
	go func() { _ = dispatcher.RegisterForUpdates(request, client) }()
	assert.True(t, pb.Equal(assignments, client.GetResponse()))
	client.Cancel()
}

func TestShardAssignmentDispatcher_MultipleNamespaces(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))

	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			"default": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server0", 0, 100),
					newShardAssignment(1, "server1", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
			"test-ns-1": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(2, "server1", 0, 100),
					newShardAssignment(3, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
			"test-ns-2": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(4, "server3", 0, 100),
					newShardAssignment(5, "server4", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
	coordinatorStream.AddRequest(request)
	// Wait for the dispatcher to process the initializing request
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	// Should get the whole assignment as they arrived from controller
	mockClient := rpc.NewMockShardAssignmentClientStream()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		req := &proto.ShardAssignmentsRequest{Namespace: "test-ns-1"}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
	})

	mockClient.Cancel()
	wg.Wait()

	response := mockClient.GetResponse()
	assert.True(t, pb.Equal(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			"test-ns-1": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(2, "server1", 0, 100),
					newShardAssignment(3, "server2", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}, response))

	// If namespace is not passed, it will use "default"
	mockClient = rpc.NewMockShardAssignmentClientStream()
	wg = sync.WaitGroup{}
	wg.Go(func() {
		req := &proto.ShardAssignmentsRequest{Namespace: ""}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.NoError(t, err)
	})

	mockClient.Cancel()
	wg.Wait()

	response = mockClient.GetResponse()
	assert.True(t, pb.Equal(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			"default": {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server0", 0, 100),
					newShardAssignment(1, "server1", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}, response))

	// If the namespace is not valid, we'll get an error
	mockClient = rpc.NewMockShardAssignmentClientStream()
	wg = sync.WaitGroup{}
	wg.Go(func() {
		req := &proto.ShardAssignmentsRequest{Namespace: "non-valid-namespace"}
		err := dispatcher.RegisterForUpdates(req, mockClient)
		assert.ErrorIs(t, err, constant.ErrNamespaceNotFound)
	})

	mockClient.Cancel()
	wg.Wait()

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_GetLeader(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))

	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1:6649", 0, 100),
					newShardAssignment(1, "server2:6649", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	})
	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	assert.Equal(t, "server1:6649", dispatcher.GetLeader(0))
	assert.Equal(t, "server2:6649", dispatcher.GetLeader(1))
	assert.Equal(t, "", dispatcher.GetLeader(999))

	// Update assignments - shard 1 moves to server3
	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1:6649", 0, 100),
					newShardAssignment(1, "server3:6649", 100, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	})

	assert.Eventually(t, func() bool {
		return dispatcher.GetLeader(1) == "server3:6649"
	}, 10*time.Second, 10*time.Millisecond)

	assert.Equal(t, "server1:6649", dispatcher.GetLeader(0))
	assert.Equal(t, "server3:6649", dispatcher.GetLeader(1))

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_HasAuthority(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))

	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	coordinatorStream.AddRequest(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1:6648", 0, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
		AllowedAuthorities: []string{"server1:6648", "server1:6649", "bootstrap:6648"},
	})

	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	assert.True(t, dispatcher.HasAuthority("server1:6648"))
	assert.True(t, dispatcher.HasAuthority("server1:6649"))
	assert.True(t, dispatcher.HasAuthority("bootstrap:6648"))
	assert.False(t, dispatcher.HasAuthority("other:6648"))

	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_ClientAssignmentsDoNotIncludeAuthorities(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))

	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	request := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{
					newShardAssignment(0, "server1:6648", 0, math.MaxUint32),
				},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
		AllowedAuthorities: []string{"server1:6648", "bootstrap:6648"},
	}
	coordinatorStream.AddRequest(request)

	assert.Eventually(t, func() bool {
		return dispatcher.Initialized()
	}, 10*time.Second, 10*time.Millisecond)

	mockClient := rpc.NewMockShardAssignmentClientStream()
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{
			Namespace: constant.DefaultNamespace,
		}, mockClient)
	}()

	response := mockClient.GetResponse()
	assert.Nil(t, response.AllowedAuthorities)
	assert.Equal(t, request.Namespaces, response.Namespaces)

	mockClient.Cancel()
	assert.NoError(t, <-done)
	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcher_BusyClientGetsLatestAssignments(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))

	coordinatorStream := rpc.NewMockShardAssignmentControllerStream()
	go func() {
		err := dispatcher.PushShardAssignments(coordinatorStream)
		assert.NoError(t, err)
	}()

	assignmentsWithLeader := func(leader string) *proto.ShardAssignments {
		return &proto.ShardAssignments{
			Namespaces: map[string]*proto.NamespaceShardsAssignment{
				constant.DefaultNamespace: {
					Assignments:    []*proto.ShardAssignment{newShardAssignment(0, leader, 0, math.MaxUint32)},
					ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
				},
			},
		}
	}
	// pushAssignments returns once the dispatcher has handed the update to the
	// clients: GetLeader only sees it after that, as both happen under the lock.
	pushAssignments := func(leader string) {
		coordinatorStream.AddRequest(assignmentsWithLeader(leader))
		require.Eventually(t, func() bool {
			return dispatcher.GetLeader(0) == leader
		}, 10*time.Second, 10*time.Millisecond)
	}

	pushAssignments("server1")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// A client that stays busy sending each update until the test reads it
	sending := make(chan struct{}, 10)
	responses := make(chan *proto.ShardAssignments)
	client := &fakeClient{ctx: ctx, send: func(assignments *proto.ShardAssignments) error {
		sending <- struct{}{}
		select {
		case responses <- assignments:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{
			Namespace: constant.DefaultNamespace,
		}, client)
	}()

	// Two updates arrive while the client is still busy sending the initial
	// assignments
	<-sending
	pushAssignments("server2")
	pushAssignments("server3")

	receive := func() *proto.ShardAssignments {
		select {
		case assignments := <-responses:
			return assignments
		case err := <-done:
			require.FailNow(t, "the dispatcher closed the client stream", "error: %v", err)
		case <-time.After(10 * time.Second):
			require.FailNow(t, "timed out waiting for the assignments")
		}
		return nil
	}
	assert.True(t, pb.Equal(assignmentsWithLeader("server1"), receive()))
	// The client is not cut off: once done with the initial assignments, it
	// sends the latest ones
	assert.True(t, pb.Equal(assignmentsWithLeader("server3"), receive()))

	cancel()
	assert.NoError(t, <-done)
	assert.NoError(t, dispatcher.Close())
}

func TestShardAssignmentDispatcherRegisterForUpdatesInvalidAuthority(t *testing.T) {
	dispatcher := NewStandaloneShardAssignmentDispatcher(1, proto.KeySortingType_UNKNOWN)
	defer dispatcher.Close()

	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(":authority", "http://server1:6648"))
	err := dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{}, &fakeClient{ctx: ctx})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestShardAssignmentDispatcherRegisterForUpdatesInitialSendError(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
	defer dispatcher.Close()
	s := dispatcher.(*shardAssignmentDispatcher)
	s.updateShardAssignment(singleShardAssignments("server1"))

	sendErr := status.Error(codes.Unavailable, "client gone")
	client := &fakeClient{ctx: t.Context(), send: func(*proto.ShardAssignments) error { return sendErr }}

	err := dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{}, client)
	assert.ErrorIs(t, err, sendErr)
	s.RLock()
	assert.Empty(t, s.clients)
	s.RUnlock()
}

func TestShardAssignmentDispatcherRegisterForUpdatesUpdateSendError(t *testing.T) {
	for _, sendErr := range []error{
		status.Error(codes.Unavailable, "client gone"),
		status.Error(codes.Canceled, "client canceled"),
	} {
		t.Run(status.Code(sendErr).String(), func(t *testing.T) {
			dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
			defer dispatcher.Close()
			s := dispatcher.(*shardAssignmentDispatcher)
			s.updateShardAssignment(singleShardAssignments("server1"))

			// gRPC puts the address of the client in the context of its stream
			ctx := peer.NewContext(t.Context(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}})
			sent := make(chan *proto.ShardAssignments, 2)
			sends := 0
			client := &fakeClient{ctx: ctx, send: func(assignments *proto.ShardAssignments) error {
				sent <- assignments
				sends++
				if sends > 1 {
					return sendErr
				}
				return nil
			}}
			done := make(chan error, 1)
			go func() {
				done <- dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{}, client)
			}()

			assert.True(t, pb.Equal(singleShardAssignments("server1"), <-sent))
			s.updateShardAssignment(singleShardAssignments("server2"))

			assert.ErrorIs(t, <-done, sendErr)
			assert.True(t, pb.Equal(singleShardAssignments("server2"), <-sent))
			s.RLock()
			assert.Empty(t, s.clients)
			s.RUnlock()
		})
	}
}

func TestShardAssignmentDispatcherRegisterForUpdatesReturnsOnClose(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
	s := dispatcher.(*shardAssignmentDispatcher)
	s.updateShardAssignment(singleShardAssignments("server1"))

	sent := make(chan struct{}, 1)
	client := &fakeClient{ctx: t.Context(), send: func(*proto.ShardAssignments) error {
		sent <- struct{}{}
		return nil
	}}
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.RegisterForUpdates(&proto.ShardAssignmentsRequest{}, client)
	}()
	<-sent

	assert.NoError(t, dispatcher.Close())
	assert.NoError(t, <-done)
}

func TestShardAssignmentDispatcherUpdateShardAssignmentDebugLog(t *testing.T) {
	dispatcher := NewShardAssignmentDispatcher(oxiadcommonrpc.NewClosableHealthServer(t.Context()))
	defer dispatcher.Close()
	s := dispatcher.(*shardAssignmentDispatcher)
	logs := &bytes.Buffer{}
	s.log = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s.updateShardAssignment(singleShardAssignments("server1"))

	// At debug level, the update logs the previous and the current assignments
	assert.Contains(t, logs.String(), "level=DEBUG")
	assert.Contains(t, logs.String(), "previous=")
	assert.Contains(t, logs.String(), "server1")
	assert.Equal(t, "server1", dispatcher.GetLeader(0))
}

func singleShardAssignments(leader string) *proto.ShardAssignments {
	return &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments:    []*proto.ShardAssignment{newShardAssignment(0, leader, 0, math.MaxUint32)},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
}

func newShardAssignment(id int64, leader string, minHashInclusive uint32, maxHashInclusive uint32) *proto.ShardAssignment {
	return &proto.ShardAssignment{
		Shard:  id,
		Leader: leader,
		ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
			Int32HashRange: &proto.Int32HashRange{
				MinHashInclusive: minHashInclusive,
				MaxHashInclusive: maxHashInclusive,
			},
		},
	}
}

type fakeClient struct {
	ctx  context.Context
	send func(assignments *proto.ShardAssignments) error
}

func (c *fakeClient) Send(assignments *proto.ShardAssignments) error {
	return c.send(assignments)
}

func (c *fakeClient) Context() context.Context {
	return c.ctx
}

// endableCoordinatorStream is a stream of the coordinator that ends when the test says so, like a lost connection.
type endableCoordinatorStream struct {
	*rpc.MockShardAssignmentControllerStream
	requests chan *proto.ShardAssignments
	ended    chan struct{}
}

func newEndableCoordinatorStream() *endableCoordinatorStream {
	return &endableCoordinatorStream{
		MockShardAssignmentControllerStream: rpc.NewMockShardAssignmentControllerStream(),
		requests:                            make(chan *proto.ShardAssignments, 10),
		ended:                               make(chan struct{}),
	}
}

func (s *endableCoordinatorStream) AddRequest(assignments *proto.ShardAssignments) {
	s.requests <- assignments
}

func (s *endableCoordinatorStream) Recv() (*proto.ShardAssignments, error) {
	select {
	case assignments := <-s.requests:
		return assignments, nil
	case <-s.ended:
		return nil, io.EOF
	}
}

func (s *endableCoordinatorStream) end() {
	close(s.ended)
}
