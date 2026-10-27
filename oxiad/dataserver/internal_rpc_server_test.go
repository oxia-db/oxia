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

package dataserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	clientrpc "github.com/oxia-db/oxia/common/rpc"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/follow"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

type internalTestAssignmentDispatcher struct {
	pushShardAssignments func(proto.OxiaCoordination_PushShardAssignmentsServer) error
}

func (*internalTestAssignmentDispatcher) Close() error { return nil }

func (*internalTestAssignmentDispatcher) Initialized() bool {
	return false
}

func (t *internalTestAssignmentDispatcher) PushShardAssignments(stream proto.OxiaCoordination_PushShardAssignmentsServer) error {
	if t.pushShardAssignments != nil {
		return t.pushShardAssignments(stream)
	}
	return stream.SendAndClose(&proto.CoordinationShardAssignmentsResponse{})
}

func (*internalTestAssignmentDispatcher) RegisterForUpdates(*proto.ShardAssignmentsRequest, assignment.Client) error {
	panic("unexpected call")
}

func (*internalTestAssignmentDispatcher) GetLeader(int64) string { return "" }

func (*internalTestAssignmentDispatcher) HasAuthority(string) bool { return false }

func newTestManifest(t *testing.T, instanceID string) *manifestpkg.Manifest {
	t.Helper()

	manifest, err := manifestpkg.NewManifest(t.TempDir())
	require.NoError(t, err)
	if instanceID == "" {
		return manifest
	}

	_, err = manifest.SetInstanceID(instanceID)
	require.NoError(t, err)
	return manifest
}

func newInternalTestServer(t *testing.T, manifest *manifestpkg.Manifest, dispatcher *internalTestAssignmentDispatcher) *internalRpcServer {
	t.Helper()

	healthServer := rpc2.NewClosableHealthServer(t.Context())
	server, err := newInternalRpcServer(rpc2.Default, "localhost:0", nil,
		dispatcher, healthServer, nil, &auth.Disabled, manifest)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, server.Close())
		assert.NoError(t, healthServer.Close())
	})

	return server
}

func newInternalTestConn(t *testing.T, server *internalRpcServer, instanceID string) *grpc.ClientConn {
	t.Helper()

	target := fmt.Sprintf("localhost:%d", server.grpcServer.Port())
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if instanceID != "" {
		options = append(options, clientrpc.MetadataInjectionDialOptions(func() map[string]string {
			return map[string]string{
				constant.MetadataInstanceId: instanceID,
			}
		})...)
	}

	cnx, err := grpc.NewClient(target, options...)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, cnx.Close())
	})

	return cnx
}

func TestInternalHealthCheckRemainsOpenWhileUninitialized(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, ""), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := grpc_health_v1.NewHealthClient(cnx)
	response, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, response.Status)
}

func TestInternalGetInfoRejectedWhileUninitialized(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, ""), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := proto.NewOxiaCoordinationClient(cnx)
	_, err := client.GetInfo(context.Background(), &proto.GetInfoRequest{}) //nolint:staticcheck // Deprecated endpoint coverage.
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, grpcstatus.Code(err))
}

func TestInternalHandshakeBindsInstanceID(t *testing.T) {
	manifest := newTestManifest(t, "")
	server := newInternalTestServer(t, manifest, &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := proto.NewOxiaCoordinationClient(cnx)
	response, err := client.Handshake(context.Background(), &proto.HandshakeRequest{InstanceId: "cluster-a"})
	require.NoError(t, err)
	assert.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_BOUND, response.Status)
	assert.Equal(t, "cluster-a", manifest.GetInstanceID())
}

func TestInternalProtectedRpcRejectedWhileUninitialized(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, ""), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := proto.NewOxiaCoordinationClient(cnx)
	stream, err := client.PushShardAssignments(context.Background())
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, grpcstatus.Code(err))
}

func TestInternalReplicateRejectsWrongInstanceID(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, "cluster-a"), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "cluster-b")

	client := proto.NewOxiaLogReplicationClient(cnx)
	stream, err := client.Replicate(context.Background())
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))
}

func TestInternalPushShardAssignmentsAcceptsMatchingInstanceID(t *testing.T) {
	var called atomic.Bool
	server := newInternalTestServer(t, newTestManifest(t, "cluster-a"), &internalTestAssignmentDispatcher{
		pushShardAssignments: func(stream proto.OxiaCoordination_PushShardAssignmentsServer) error {
			called.Store(true)
			return stream.SendAndClose(&proto.CoordinationShardAssignmentsResponse{})
		},
	})
	cnx := newInternalTestConn(t, server, "cluster-a")

	client := proto.NewOxiaCoordinationClient(cnx)
	stream, err := client.PushShardAssignments(context.Background())
	require.NoError(t, err)

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	assert.NotNil(t, response)
	assert.True(t, called.Load())
}

func TestInternalGetInfoAcceptsMatchingInstanceID(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, "cluster-a"), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "cluster-a")

	client := proto.NewOxiaCoordinationClient(cnx)
	response, err := client.GetInfo(context.Background(), &proto.GetInfoRequest{}) //nolint:staticcheck // Deprecated endpoint coverage.
	require.NoError(t, err)
	assert.NotNil(t, response)
}

func TestInternalHandshakeReturnsMismatchForDifferentInstanceID(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, "cluster-a"), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := proto.NewOxiaCoordinationClient(cnx)
	response, err := client.Handshake(context.Background(), &proto.HandshakeRequest{InstanceId: "cluster-b"})
	require.NoError(t, err)
	assert.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_MISMATCH, response.Status)
}

func TestReadSplitHashRange(t *testing.T) {
	splitMD := func(lo, hi string) metadata.MD {
		return metadata.New(map[string]string{
			constant.MetadataSplitHashRangeMin: lo,
			constant.MetadataSplitHashRangeMax: hi,
		})
	}

	t.Run("absent", func(t *testing.T) {
		hr, ok, err := readSplitHashRange(metadata.New(nil))
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, hr)
	})

	t.Run("valid", func(t *testing.T) {
		hr, ok, err := readSplitHashRange(splitMD("100", "200"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(100), hr.GetMin())
		assert.Equal(t, uint32(200), hr.GetMax())
	})

	t.Run("single-value range", func(t *testing.T) {
		hr, ok, err := readSplitHashRange(splitMD("42", "42"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(42), hr.GetMin())
		assert.Equal(t, uint32(42), hr.GetMax())
	})

	// A reversed range makes isHashInRange false for every hash, so the split
	// filter would delete the child's entire dataset. Reject it at the parse
	// boundary instead.
	t.Run("reversed range rejected", func(t *testing.T) {
		hr, ok, err := readSplitHashRange(splitMD("200", "100"))
		require.Error(t, err)
		assert.False(t, ok)
		assert.Nil(t, hr)
	})
}

func TestReadSplitParentHashRange(t *testing.T) {
	splitMD := func(lo, hi string) metadata.MD {
		return metadata.New(map[string]string{
			constant.MetadataSplitHashRangeMin:       "100",
			constant.MetadataSplitHashRangeMax:       "200",
			constant.MetadataSplitParentHashRangeMin: lo,
			constant.MetadataSplitParentHashRangeMax: hi,
		})
	}

	// From a coordinator that predates it
	hr, err := readSplitParentHashRange(metadata.New(map[string]string{
		constant.MetadataSplitHashRangeMin: "100",
		constant.MetadataSplitHashRangeMax: "200",
	}))
	require.NoError(t, err)
	assert.Nil(t, hr)

	hr, err = readSplitParentHashRange(splitMD("100", "300"))
	require.NoError(t, err)
	assert.Equal(t, uint32(100), hr.GetMin())
	assert.Equal(t, uint32(300), hr.GetMax())

	hr, err = readSplitParentHashRange(splitMD("300", "100"))
	require.Error(t, err)
	assert.Nil(t, hr)
}

func TestNewInternalRpcServerDefaultAuth(t *testing.T) {
	healthServer := rpc2.NewClosableHealthServer(t.Context())
	t.Cleanup(func() { assert.NoError(t, healthServer.Close()) })
	manifest := newTestManifest(t, "")

	// Without auth options, the server doesn't authenticate the requests
	server, err := newInternalRpcServer(rpc2.Default, "localhost:0", nil, &internalTestAssignmentDispatcher{},
		healthServer, nil, nil, manifest)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, server.Close()) })

	client := proto.NewOxiaCoordinationClient(newInternalTestConn(t, server, ""))
	response, err := client.Handshake(context.Background(), &proto.HandshakeRequest{InstanceId: "cluster-a"})
	require.NoError(t, err)
	assert.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_BOUND, response.Status)
}

func TestNewInternalRpcServerBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	healthServer := rpc2.NewClosableHealthServer(t.Context())
	t.Cleanup(func() { assert.NoError(t, healthServer.Close()) })

	server, err := newInternalRpcServer(rpc2.Default, listener.Addr().String(), nil, &internalTestAssignmentDispatcher{},
		healthServer, nil, &auth.Disabled, newTestManifest(t, ""))
	assert.ErrorContains(t, err, listener.Addr().String())
	assert.Nil(t, server)
}

func TestInternalPushShardAssignmentsError(t *testing.T) {
	for _, pushErr := range []error{
		grpcstatus.Error(codes.Unavailable, "coordinator gone"),
		grpcstatus.Error(codes.Canceled, "coordinator canceled"),
	} {
		server := &internalRpcServer{
			log: slog.Default(),
			assignmentDispatcher: &internalTestAssignmentDispatcher{
				pushShardAssignments: func(proto.OxiaCoordination_PushShardAssignmentsServer) error { return pushErr },
			},
		}

		err := server.PushShardAssignments(
			&testStream[proto.ShardAssignments, proto.CoordinationShardAssignmentsResponse]{ctx: context.Background()})
		assert.ErrorIs(t, err, pushErr)
	}
}

func TestInternalHandshakeRejectsEmptyInstanceID(t *testing.T) {
	manifest := newTestManifest(t, "")
	server := newInternalTestServer(t, manifest, &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := proto.NewOxiaCoordinationClient(cnx)
	_, err := client.Handshake(context.Background(), &proto.HandshakeRequest{})
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
	assert.Empty(t, manifest.GetInstanceID())
}

func TestInternalHandshakeAlreadyBound(t *testing.T) {
	server := newInternalTestServer(t, newTestManifest(t, "cluster-a"), &internalTestAssignmentDispatcher{})
	cnx := newInternalTestConn(t, server, "")

	client := proto.NewOxiaCoordinationClient(cnx)
	response, err := client.Handshake(context.Background(), &proto.HandshakeRequest{InstanceId: "cluster-a"})
	require.NoError(t, err)
	assert.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND, response.Status)
}

func TestInternalHandshakeFailsToBindInstanceID(t *testing.T) {
	dir := t.TempDir()
	manifest, err := manifestpkg.NewManifest(dir)
	require.NoError(t, err)
	// The manifest can't be written
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
	server := &internalRpcServer{log: slog.Default(), manifest: manifest}

	_, err = server.Handshake(context.Background(), &proto.HandshakeRequest{InstanceId: "cluster-a"})
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.Empty(t, manifest.GetInstanceID())
}

func TestInternalNewTermFollowerError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getFollower: func(int64) (follow.FollowerController, error) {
			return &testFollowerController{
				newTerm: func(*proto.NewTermRequest) (*proto.NewTermResponse, error) {
					return nil, constant.ErrInvalidTerm
				},
			}, nil
		},
	})

	_, err := server.NewTerm(context.Background(), &proto.NewTermRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrInvalidTerm)
}

func TestInternalNewTermLeaderError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getFollower: notFollower,
		getOrCreateLeader: func(string, int64) (lead.LeaderController, error) {
			return &testLeaderController{
				newTerm: func(*proto.NewTermRequest) (*proto.NewTermResponse, error) {
					return nil, constant.ErrInvalidTerm
				},
			}, nil
		},
	})

	_, err := server.NewTerm(context.Background(), &proto.NewTermRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrInvalidTerm)
}

func TestInternalBecomeLeaderError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateLeader: func(string, int64) (lead.LeaderController, error) {
			return &testLeaderController{
				becomeLeader: func(context.Context, *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
					return nil, constant.ErrInvalidStatus
				},
			}, nil
		},
	})

	_, err := server.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrInvalidStatus)
}

func TestInternalNewTermGetOrCreateLeaderError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getFollower:       notFollower,
		getOrCreateLeader: closedDirectorLeader,
	})

	_, err := server.NewTerm(context.Background(), &proto.NewTermRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrResourceUnavailable)
}

func TestInternalBecomeLeaderGetOrCreateLeaderError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{getOrCreateLeader: closedDirectorLeader})

	_, err := server.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrResourceUnavailable)
}

func TestInternalAddFollower(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{getLeader: notLeader})
	_, err := server.AddFollower(context.Background(), &proto.AddFollowerRequest{Shard: 1})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)

	server = newDirectorTestInternalServer(&testShardsDirector{
		getLeader: func(int64) (lead.LeaderController, error) {
			return &testLeaderController{
				addFollower: func(*proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
					return nil, constant.ErrInvalidTerm
				},
			}, nil
		},
	})
	_, err = server.AddFollower(context.Background(), &proto.AddFollowerRequest{Shard: 1})
	assertOxiaError(t, err, constant.ErrInvalidTerm)
}

func TestInternalRemoveObserver(t *testing.T) {
	var removed *proto.RemoveObserverRequest
	server := newDirectorTestInternalServer(&testShardsDirector{
		getLeader: func(int64) (lead.LeaderController, error) {
			return &testLeaderController{
				removeObserver: func(req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
					if req.Term != 1 {
						return nil, constant.ErrInvalidTerm
					}
					removed = req
					return &proto.RemoveObserverResponse{}, nil
				},
			}, nil
		},
	})

	request := &proto.RemoveObserverRequest{Shard: 1, Term: 1, FollowerName: "observer:6649", TargetShard: 2}
	res, err := server.RemoveObserver(context.Background(), request)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Same(t, request, removed)

	_, err = server.RemoveObserver(context.Background(), &proto.RemoveObserverRequest{Shard: 1, Term: 2})
	assertOxiaError(t, err, constant.ErrInvalidTerm)

	server = newDirectorTestInternalServer(&testShardsDirector{getLeader: notLeader})
	_, err = server.RemoveObserver(context.Background(), request)
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)
}

func TestInternalFreezeShardError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getLeader: func(int64) (lead.LeaderController, error) {
			return &testLeaderController{
				freeze: func(*proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
					return nil, constant.ErrInvalidTerm
				},
			}, nil
		},
	})

	_, err := server.FreezeShard(context.Background(), &proto.FreezeShardRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrInvalidTerm)
}

func TestInternalFreezeShardGetLeaderError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{getLeader: notLeader})

	_, err := server.FreezeShard(context.Background(), &proto.FreezeShardRequest{Shard: 1, Term: 1})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)
}

func TestInternalTruncate(t *testing.T) {
	var follower string
	server := newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateFollower: func(namespace string, shard int64, term int64) (follow.FollowerController, error) {
			follower = fmt.Sprintf("%s/%d/%d", namespace, shard, term)
			return &testFollowerController{
				truncate: func(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
					if req.HeadEntryId == nil {
						return nil, constant.ErrInvalidStatus
					}
					return &proto.TruncateResponse{HeadEntryId: req.HeadEntryId}, nil
				},
			}, nil
		},
	})

	res, err := server.Truncate(context.Background(), &proto.TruncateRequest{
		Namespace: constant.DefaultNamespace, Shard: 1, Term: 2, HeadEntryId: &proto.EntryId{Term: 1, Offset: 5},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 5, res.HeadEntryId.Offset)
	assert.Equal(t, constant.DefaultNamespace+"/1/2", follower)

	_, err = server.Truncate(context.Background(), &proto.TruncateRequest{Namespace: constant.DefaultNamespace, Shard: 1})
	assertOxiaError(t, err, constant.ErrInvalidStatus)

	server = newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateFollower: func(string, int64, int64) (follow.FollowerController, error) {
			return nil, constant.ErrInvalidTerm
		},
	})
	_, err = server.Truncate(context.Background(), &proto.TruncateRequest{Namespace: constant.DefaultNamespace, Shard: 1})
	assertOxiaError(t, err, constant.ErrInvalidTerm)
}

func TestInternalReplicateInvalidMetadata(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateFollower: func(string, int64, int64) (follow.FollowerController, error) {
			return &testFollowerController{}, nil
		},
	})

	for _, tt := range invalidReplicationMetadata() {
		t.Run(tt.name, func(t *testing.T) {
			err := server.Replicate(&testStream[proto.Append, proto.Ack]{ctx: tt.ctx})
			assert.ErrorContains(t, err, tt.err)
		})
	}
}

func TestInternalReplicateFollowerError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateFollower: func(string, int64, int64) (follow.FollowerController, error) {
			return nil, constant.ErrResourceUnavailable
		},
	})

	err := server.Replicate(&testStream[proto.Append, proto.Ack]{
		ctx: replicationContext(constant.MetadataTerm, "2"),
	})
	assertOxiaError(t, err, constant.ErrResourceUnavailable)
}

func TestInternalSendSnapshotInvalidMetadata(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateFollower: func(string, int64, int64) (follow.FollowerController, error) {
			return &testFollowerController{}, nil
		},
	})

	for _, tt := range invalidReplicationMetadata() {
		t.Run(tt.name, func(t *testing.T) {
			err := server.SendSnapshot(&testStream[proto.SnapshotChunk, proto.SnapshotResponse]{ctx: tt.ctx})
			assert.ErrorContains(t, err, tt.err)
		})
	}
}

func TestInternalSendSnapshotFollowerError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getOrCreateFollower: func(string, int64, int64) (follow.FollowerController, error) {
			return nil, constant.ErrInvalidTerm
		},
	})

	err := server.SendSnapshot(&testStream[proto.SnapshotChunk, proto.SnapshotResponse]{
		ctx: replicationContext(constant.MetadataTerm, "2"),
	})
	assertOxiaError(t, err, constant.ErrInvalidTerm)
}

func TestInternalGetStatusFollowerError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getFollower: func(int64) (follow.FollowerController, error) {
			return nil, constant.ErrResourceUnavailable
		},
	})

	_, err := server.GetStatus(context.Background(), &proto.GetStatusRequest{Shard: 1})
	assertOxiaError(t, err, constant.ErrResourceUnavailable)
}

func TestInternalGetStatusDirectorError(t *testing.T) {
	server := newDirectorTestInternalServer(&testShardsDirector{
		getFollower: notFollower,
		getLeader: func(int64) (lead.LeaderController, error) {
			return nil, constant.ErrResourceUnavailable
		},
	})

	_, err := server.GetStatus(context.Background(), &proto.GetStatusRequest{Shard: 1})
	assertOxiaError(t, err, constant.ErrResourceUnavailable)
}

func TestReadHeader(t *testing.T) {
	value, err := readHeader(metadata.Pairs("key", "value"), "key")
	require.NoError(t, err)
	assert.Equal(t, "value", value)

	_, err = readHeader(metadata.Pairs(), "key")
	assert.ErrorContains(t, err, "must include 'key'")

	_, err = readHeader(metadata.Pairs("key", "a", "key", "b"), "key")
	assert.ErrorContains(t, err, "only once")
}

func TestReadHeaderInt64(t *testing.T) {
	value, err := ReadHeaderInt64(metadata.Pairs("key", "42"), "key")
	require.NoError(t, err)
	assert.EqualValues(t, 42, value)

	_, err = ReadHeaderInt64(metadata.Pairs("key", "not-a-number"), "key")
	assert.Error(t, err)
}

func TestReadTerm(t *testing.T) {
	term, err := readTerm(metadata.Pairs(constant.MetadataTerm, "3"))
	require.NoError(t, err)
	assert.EqualValues(t, 3, term)

	// A request of a former version doesn't include the term
	term, err = readTerm(metadata.Pairs())
	require.NoError(t, err)
	assert.EqualValues(t, -1, term)
}

func TestReadHashRangeInvalidNumbers(t *testing.T) {
	_, _, err := readSplitHashRange(metadata.New(map[string]string{
		constant.MetadataSplitHashRangeMin: "low",
		constant.MetadataSplitHashRangeMax: "200",
	}))
	assert.ErrorContains(t, err, "invalid split hash range min")

	_, _, err = readSplitHashRange(metadata.New(map[string]string{
		constant.MetadataSplitHashRangeMin: "100",
		constant.MetadataSplitHashRangeMax: "high",
	}))
	assert.ErrorContains(t, err, "invalid split hash range max")
}

func TestSetSplitHashRange(t *testing.T) {
	follower := &testFollowerController{}
	require.NoError(t, setSplitHashRange(follower, metadata.New(map[string]string{
		constant.MetadataSplitHashRangeMin:       "100",
		constant.MetadataSplitHashRangeMax:       "200",
		constant.MetadataSplitParentHashRangeMin: "0",
		constant.MetadataSplitParentHashRangeMax: "300",
	}), 2))
	assert.Equal(t, []uint32{100, 200}, []uint32{follower.splitHashRange.GetMin(), follower.splitHashRange.GetMax()})
	assert.Equal(t, []uint32{0, 300},
		[]uint32{follower.splitParentHashRange.GetMin(), follower.splitParentHashRange.GetMax()})

	// Not a split child
	follower = &testFollowerController{}
	require.NoError(t, setSplitHashRange(follower, metadata.New(nil), 2))
	assert.Nil(t, follower.splitHashRange)
}

func newDirectorTestInternalServer(shardsDirector controller.ShardsDirector) *internalRpcServer {
	return &internalRpcServer{log: slog.Default(), shardsDirector: shardsDirector}
}

// replicationContext returns the context of a replication stream of shard 1
// of the default namespace, with the given metadata in addition.
func replicationContext(keyValues ...string) context.Context {
	md := metadata.Pairs(constant.MetadataShardId, "1", constant.MetadataNamespace, constant.DefaultNamespace)
	return metadata.NewIncomingContext(context.Background(), metadata.Join(md, metadata.Pairs(keyValues...)))
}

func invalidReplicationMetadata() []struct {
	name string
	ctx  context.Context
	err  string
} {
	return []struct {
		name string
		ctx  context.Context
		err  string
	}{
		{name: "no metadata", ctx: context.Background(), err: "shard id is not set"},
		{name: "no shard", ctx: metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(constant.MetadataNamespace, constant.DefaultNamespace)), err: constant.MetadataShardId},
		{name: "no namespace", ctx: metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(constant.MetadataShardId, "1")), err: constant.MetadataNamespace},
		{name: "invalid term", ctx: replicationContext(constant.MetadataTerm, "invalid"), err: "expected integer"},
		{name: "invalid split hash range", ctx: replicationContext(
			constant.MetadataSplitHashRangeMin, "200", constant.MetadataSplitHashRangeMax, "100"),
			err: "invalid split hash range"},
		{name: "invalid split parent hash range", ctx: replicationContext(
			constant.MetadataSplitHashRangeMin, "100", constant.MetadataSplitHashRangeMax, "200",
			constant.MetadataSplitParentHashRangeMin, "300", constant.MetadataSplitParentHashRangeMax, "0"),
			err: "invalid split parent hash range"},
	}
}

func notFollower(int64) (follow.FollowerController, error) {
	return nil, errs.ErrNotFollower
}

func notLeader(int64) (lead.LeaderController, error) {
	return nil, constant.ErrNodeIsNotLeader
}

// closedDirectorLeader fails to create a leader, as a closed shards director.
func closedDirectorLeader(string, int64) (lead.LeaderController, error) {
	return nil, constant.ErrResourceUnavailable
}

type testFollowerController struct {
	follow.FollowerController
	newTerm              func(*proto.NewTermRequest) (*proto.NewTermResponse, error)
	truncate             func(*proto.TruncateRequest) (*proto.TruncateResponse, error)
	splitHashRange       *proto.HashRange
	splitParentHashRange *proto.HashRange
}

func (f *testFollowerController) Term() int64 { return 0 }

func (f *testFollowerController) NewTerm(req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	return f.newTerm(req)
}

func (f *testFollowerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	return f.truncate(req)
}

func (f *testFollowerController) SetSplitHashRange(hashRange *proto.HashRange, parentHashRange *proto.HashRange, _ int64) {
	f.splitHashRange = hashRange
	f.splitParentHashRange = parentHashRange
}
