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

package dataserver

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	clientrpc "github.com/oxia-db/oxia/common/rpc"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
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

	require.NoError(t, manifest.SetInstanceID(instanceID))
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
		options = append(options, clientrpc.MetadataInjectionDialOptions(map[string]string{
			constant.MetadataInstanceId: instanceID,
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
	_, err := client.GetInfo(context.Background(), &proto.GetInfoRequest{})
	require.Error(t, err)
	assert.Equal(t, constant.CodeNotInitialized, grpcstatus.Code(err))
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
	assert.Equal(t, constant.CodeNotInitialized, grpcstatus.Code(err))
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
	response, err := client.GetInfo(context.Background(), &proto.GetInfoRequest{})
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
