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

package shard

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

// restoreRPCProvider is the test provider with the backup service. It
// answers RestoreShard with the scripted responses in order, repeating the
// last one, and records the requests.
type restoreRPCProvider struct {
	*rpctest.RpcProvider
	mu        sync.Mutex
	responses []*proto.RestoreShardResponse
	err       error
	requests  []*proto.RestoreShardRequest
}

var _ coordrpc.BackupProvider = (*restoreRPCProvider)(nil)

func (p *restoreRPCProvider) RestoreShard(_ context.Context, _ *proto.DataServerIdentity,
	req *proto.RestoreShardRequest) (*proto.RestoreShardResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	if p.err != nil {
		return nil, p.err
	}
	res := p.responses[0]
	if len(p.responses) > 1 {
		p.responses = p.responses[1:]
	}
	return res, nil
}

func (p *restoreRPCProvider) recorded() []*proto.RestoreShardRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*proto.RestoreShardRequest(nil), p.requests...)
}

func (*restoreRPCProvider) BackupShard(context.Context, *proto.DataServerIdentity, *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
	return &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED}, nil
}

func (*restoreRPCProvider) ExpireBackups(context.Context, *proto.DataServerIdentity, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
	return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE}, nil
}

func (*restoreRPCProvider) ListBackups(context.Context, *proto.DataServerIdentity, *proto.ListBackupsRequest,
	func(*proto.ListBackupsResponse) error) error {
	return nil
}

func restoreResponse(state proto.BackupJobState) *proto.RestoreShardResponse {
	return &proto.RestoreShardResponse{State: state}
}

// restoringShard is the metadata of a shard created by a restore, before its
// first election.
func restoringShard(members ...*proto.DataServerIdentity) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         proto.ShardStatusUnknown,
		Term:           -1,
		Ensemble:       members,
		Int32HashRange: &proto.HashRange{Min: 0, Max: 1<<31 - 1},
		Restore: &proto.RestoreMetadata{
			Destination:     "s3://bucket/prefix",
			SourceNamespace: "orders",
			SourceShardId:   3,
			BackupId:        "2026-10-03T14-00-00Z-abcd",
		},
	}
}

// newRestoringController stores the shard metadata and builds a controller
// of the shard for restoreShard, without starting its event loop.
func newRestoringController(t *testing.T, rpcProvider coordrpc.ControllerRPCProvider, stored *proto.ShardMetadata) (*controller, coordmetadata.Metadata) {
	t.Helper()
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, 5, namespaceConfig, stored)
	s := &controller{
		namespace:     constant.DefaultNamespace,
		shard:         5,
		metadataStore: metadata,
		rpc:           rpcProvider,
		logger:        slog.Default(),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	t.Cleanup(s.ctxCancel)
	return s, metadata
}

func TestControllerRestoreShardNeedsABackupProvider(t *testing.T) {
	s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	s, _ := newRestoringController(t, rpctest.NewRpcProvider(), restoringShard(s1))
	assert.ErrorIs(t, s.restoreShard(restoringShard(s1)), ErrRestoreUnsupported)
}

func TestControllerRestoreShardInstallsTheBackupOnEveryMemberThenClearsTheMark(t *testing.T) {
	s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Name: new("s2"), Public: "s2:9091", Internal: "s2:8191"}
	provider := &restoreRPCProvider{RpcProvider: rpctest.NewRpcProvider(), responses: []*proto.RestoreShardResponse{
		restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_STARTED),
		restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_RUNNING),
		{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE, CommitOffset: 9, Term: 2},
	}}
	shardMeta := restoringShard(s1, s2)
	s, metadata := newRestoringController(t, provider, shardMeta)

	require.NoError(t, s.restoreShard(shardMeta))
	assert.Nil(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, 5).GetRestore())
	requests := provider.recorded()
	assert.GreaterOrEqual(t, len(requests), 3, "the members are polled until the installation ends")
	want := &proto.RestoreShardRequest{
		Namespace:       constant.DefaultNamespace,
		Shard:           5,
		HashRange:       shardMeta.GetInt32HashRange(),
		Destination:     "s3://bucket/prefix",
		SourceNamespace: "orders",
		SourceShard:     3,
		BackupId:        "2026-10-03T14-00-00Z-abcd",
	}
	for _, req := range requests {
		assert.True(t, gproto.Equal(want, req), "request %v", req)
	}
}

func TestControllerRestoreShardReportsAMemberThatFails(t *testing.T) {
	errUnavailable := errors.New("data server unavailable")
	for name, test := range map[string]struct {
		provider *restoreRPCProvider
		want     error
		message  string
	}{
		"call": {&restoreRPCProvider{err: errUnavailable}, errUnavailable, "restore the shard on s1"},
		"installation": {&restoreRPCProvider{responses: []*proto.RestoreShardResponse{
			{State: proto.BackupJobState_BACKUP_JOB_STATE_FAILED, Error: "disk full"}}}, nil, "disk full"},
		"state": {&restoreRPCProvider{responses: []*proto.RestoreShardResponse{
			restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED)}}, nil, "unexpected state"},
	} {
		t.Run(name, func(t *testing.T) {
			s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
			test.provider.RpcProvider = rpctest.NewRpcProvider()
			shardMeta := restoringShard(s1)
			s, metadata := newRestoringController(t, test.provider, shardMeta)

			err := s.restoreShard(shardMeta)
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
			}
			assert.ErrorContains(t, err, test.message)
			assert.NotNil(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, 5).GetRestore(),
				"the mark stays for the retry")
		})
	}
}

func TestControllerRestoreShardStopsWithTheController(t *testing.T) {
	s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	provider := &restoreRPCProvider{RpcProvider: rpctest.NewRpcProvider(), responses: []*proto.RestoreShardResponse{
		restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_RUNNING)}}
	shardMeta := restoringShard(s1)
	s, _ := newRestoringController(t, provider, shardMeta)
	s.ctxCancel()
	assert.ErrorIs(t, s.restoreShard(shardMeta), context.Canceled)
}

func TestControllerRestoreShardLeavesAShardWhoseMarkIsGone(t *testing.T) {
	s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	provider := &restoreRPCProvider{RpcProvider: rpctest.NewRpcProvider(), responses: []*proto.RestoreShardResponse{
		restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_DONE)}}
	shardMeta := restoringShard(s1)
	stored := gproto.CloneOf(shardMeta)
	stored.Restore = nil
	s, metadata := newRestoringController(t, provider, stored)

	require.NoError(t, s.restoreShard(shardMeta))
	assert.Nil(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, 5).GetRestore())
}

func TestControllerRestoreShardReportsAMetadataFailure(t *testing.T) {
	s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	provider := &restoreRPCProvider{RpcProvider: rpctest.NewRpcProvider(), responses: []*proto.RestoreShardResponse{
		restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_DONE)}}
	shardMeta := restoringShard(s1)
	s, metadata := newRestoringController(t, provider, shardMeta)
	failing := &patchFailingMetadata{Metadata: metadata}
	failing.failures.Store(1)
	s.metadataStore = failing
	assert.ErrorContains(t, s.restoreShard(shardMeta), "clear the restore of the shard")
}

func TestControllerRestoresTheShardBeforeItsFirstElection(t *testing.T) {
	var shard int64 = 5
	s1 := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	rpc := rpctest.NewRpcProvider()
	// The first installation fails: the controller retries the restore
	provider := &restoreRPCProvider{RpcProvider: rpc, responses: []*proto.RestoreShardResponse{
		{State: proto.BackupJobState_BACKUP_JOB_STATE_FAILED, Error: "disk full"},
		restoreResponse(proto.BackupJobState_BACKUP_JOB_STATE_DONE),
	}}
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, restoringShard(s1),
		newTestFeaturesSupplier().supply, provider, DefaultPeriodicTasksInterval)
	t.Cleanup(func() { assert.NoError(t, sc.Close()) })
	rpc.GetNode(s1).NewTermResponse(0, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	assert.Eventually(t, func() bool {
		meta, ok := testShardMetadata(metadata, constant.DefaultNamespace, shard)
		return ok && meta.GetRestore() == nil && meta.GetStatusOrDefault() == proto.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)
	assert.Len(t, provider.recorded(), 2, "the failed installation is retried")
}
