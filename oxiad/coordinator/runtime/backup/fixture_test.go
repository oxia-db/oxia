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

package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

var (
	server1 = &proto.DataServerIdentity{Name: ptr("s1"), Public: "s1:6648", Internal: "s1:6649"}
	server2 = &proto.DataServerIdentity{Name: ptr("s2"), Public: "s2:6648", Internal: "s2:6649"}
	server3 = &proto.DataServerIdentity{Name: ptr("s3"), Public: "s3:6648", Internal: "s3:6649"}
	servers = []*proto.DataServerIdentity{server1, server2, server3}
)

func ptr[T any](v T) *T {
	return &v
}

type backupCall struct {
	node string
	req  *proto.BackupShardRequest
}

type expireCall struct {
	node string
	req  *proto.ExpireBackupsRequest
}

// fakeRPC answers the scheduler's calls from its hooks, and records them.
type fakeRPC struct {
	mu          sync.Mutex
	statuses    map[string]*proto.GetStatusResponse
	statusErrs  map[string]error
	backup      func(node string, req *proto.BackupShardRequest) (*proto.BackupShardResponse, error)
	expire      func(node string, req *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error)
	backupCalls []backupCall
	expireCalls []expireCall
}

func newFakeRPC() *fakeRPC {
	return &fakeRPC{
		statuses:   map[string]*proto.GetStatusResponse{},
		statusErrs: map[string]error{},
		backup: func(string, *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
			return &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_STARTED}, nil
		},
		expire: func(string, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
			return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE}, nil
		},
	}
}

func (f *fakeRPC) GetStatus(_ context.Context, node *proto.DataServerIdentity,
	_ *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.statusErrs[node.GetNameOrDefault()]; err != nil {
		return nil, err
	}
	if status, ok := f.statuses[node.GetNameOrDefault()]; ok {
		return status, nil
	}
	return nil, errors.New("no status for " + node.GetNameOrDefault())
}

func (f *fakeRPC) BackupShard(_ context.Context, node *proto.DataServerIdentity,
	req *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
	f.mu.Lock()
	hook := f.backup
	f.backupCalls = append(f.backupCalls, backupCall{node: node.GetNameOrDefault(), req: req})
	f.mu.Unlock()
	return hook(node.GetNameOrDefault(), req)
}

func (f *fakeRPC) ExpireBackups(_ context.Context, node *proto.DataServerIdentity,
	req *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
	f.mu.Lock()
	hook := f.expire
	f.expireCalls = append(f.expireCalls, expireCall{node: node.GetNameOrDefault(), req: req})
	f.mu.Unlock()
	return hook(node.GetNameOrDefault(), req)
}

func (f *fakeRPC) setBackup(hook func(node string, req *proto.BackupShardRequest) (*proto.BackupShardResponse, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backup = hook
}

func (f *fakeRPC) setExpire(hook func(node string, req *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expire = hook
}

func (f *fakeRPC) setStatus(node string, status proto.ServingStatus, commitOffset int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[node] = &proto.GetStatusResponse{Status: status, CommitOffset: commitOffset}
}

func (f *fakeRPC) backups() []backupCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]backupCall(nil), f.backupCalls...)
}

func (f *fakeRPC) expires() []expireCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]expireCall(nil), f.expireCalls...)
}

// fixture is a scheduler on a cluster of three data servers, with one
// namespace of one shard led by s1, and a backup policy enabled.
type fixture struct {
	t         *testing.T
	metadata  coordmetadata.Metadata
	rpc       *fakeRPC
	clock     *testClock
	scheduler *Scheduler
	// down are the data servers that are not available
	down map[string]bool
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFixture(t *testing.T, policy *proto.BackupConfig) *fixture {
	t.Helper()
	return newFixtureWithNamespaces(t, policy, &proto.Namespace{Name: "ns", InitialShardCount: 1, ReplicationFactor: 3})
}

// newFixtureWithNamespaces is newFixture with the given namespaces in the
// configuration; the first one is "ns", with the shard 1.
func newFixtureWithNamespaces(t *testing.T, policy *proto.BackupConfig, namespaces ...*proto.Namespace) *fixture {
	t.Helper()
	config := &proto.ClusterConfiguration{
		Namespaces: namespaces,
		Servers:    servers,
		Backup:     policy,
	}
	f := &fixture{
		t:        t,
		metadata: newTestMetadata(t, config),
		rpc:      newFakeRPC(),
		clock:    &testClock{now: time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)},
		down:     map[string]bool{},
	}
	f.scheduler = New(Options{
		Metadata:  f.metadata,
		RPC:       f.rpc,
		Available: func(name string) bool { return !f.down[name] },
		Clock:     f.clock.Now,
	})
	t.Cleanup(f.scheduler.Close)
	require.NoError(t, f.metadata.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{
			1: steadyShard(server1, servers...),
		},
	}))
	return f
}

func steadyShard(leader *proto.DataServerIdentity, ensemble ...*proto.DataServerIdentity) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         proto.ShardStatusSteadyState,
		Term:           1,
		Leader:         leader,
		Ensemble:       ensemble,
		Int32HashRange: &proto.HashRange{Min: 0, Max: ^uint32(0)},
	}
}

func enabledPolicy() *proto.BackupConfig {
	return &proto.BackupConfig{Enabled: true, Destination: "mem://test", Interval: "1h", Retention: "7d",
		ScheduleInterval: "30s", CleanupInterval: "24h"}
}

func (f *fixture) shard(id int64) *proto.ShardMetadata {
	f.t.Helper()
	borrowed, ok, err := f.metadata.GetShardStatus("ns", id)
	require.NoError(f.t, err)
	require.True(f.t, ok)
	return borrowed.UnsafeBorrow()
}

func (f *fixture) patchShard(id int64, modify func(meta *proto.ShardMetadata)) {
	f.t.Helper()
	require.NoError(f.t, f.metadata.PatchNamespaceStatus("ns", func(status *proto.NamespaceStatus) bool {
		meta, ok := status.Shards[id]
		if !ok {
			meta = steadyShard(server1, servers...)
			status.Shards[id] = meta
		}
		modify(meta)
		return true
	}))
}

func (f *fixture) state(id int64) *shardState {
	f.scheduler.mu.Lock()
	defer f.scheduler.mu.Unlock()
	return f.scheduler.shards[shardKey{namespace: "ns", shard: id}]
}

func newTestMetadata(t *testing.T, config *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()
	dir := t.TempDir()
	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileConfigName), data, 0o600))
	metadataFactory, err := coordmetadata.New(t.Context(), &coordoption.Options{
		Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File:         coordoption.FileMetadata{Dir: dir},
			},
		},
	})
	require.NoError(t, err)
	metadata, err := metadataFactory.CreateMetadata(t.Context())
	require.NoError(t, err)
	_, err = metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, metadata.Close())
		assert.NoError(t, metadataFactory.Close())
	})
	return metadata
}
