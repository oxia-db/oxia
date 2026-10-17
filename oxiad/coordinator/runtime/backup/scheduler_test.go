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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSchedulerRoundDoesNothingWhileDisabled(t *testing.T) {
	f := newFixture(t, &proto.BackupConfig{Enabled: false, Destination: "mem://test"})
	f.scheduler.round()
	assert.Empty(t, f.rpc.backups())
	assert.Empty(t, f.rpc.expires())
	assert.Nil(t, f.shard(1).GetBackupReplica())
}

func TestSchedulerRoundReturnsTheScheduleInterval(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	assert.Equal(t, 30*time.Second, f.scheduler.round())
}

func TestSchedulerRoundSkipsAShardInElection(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(1, func(meta *proto.ShardMetadata) {
		meta.Status = proto.ShardStatusElection
		meta.Leader = nil
	})
	f.scheduler.round()
	assert.Empty(t, f.rpc.backups())
	assert.NotNil(t, f.state(1), "the shard is tracked, and due")
	assert.False(t, f.state(1).nextDue.After(f.clock.Now()))
}

func TestSchedulerRoundSkipsANamespaceThatOptedOut(t *testing.T) {
	optedOut := false
	f := newFixtureWithNamespaces(t, enabledPolicy(),
		&proto.Namespace{Name: "ns", InitialShardCount: 1, ReplicationFactor: 3},
		&proto.Namespace{Name: "scratch", InitialShardCount: 1, ReplicationFactor: 3, Backup: &optedOut})
	require.NoError(t, f.metadata.CreateNamespaceStatus("scratch", &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards:            map[int64]*proto.ShardMetadata{2: steadyShard(server1, servers...)},
	}))
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()
	var backedUp []string
	for _, call := range f.rpc.backups() {
		backedUp = append(backedUp, call.req.GetNamespace())
	}
	assert.Equal(t, []string{"ns"}, backedUp, "the shards of the namespace that opted out take no backups")
	f.scheduler.mu.Lock()
	_, tracked := f.scheduler.shards[shardKey{namespace: "scratch", shard: 2}]
	f.scheduler.mu.Unlock()
	assert.False(t, tracked, "the namespace that opted out reports no age")
	assert.Equal(t, 0, f.scheduler.RequestBackup("scratch", nil), "a forced backup doesn't override the opt-out")
	var cleaned []string
	for _, call := range f.rpc.expires() {
		cleaned = append(cleaned, call.req.GetNamespace())
	}
	assert.ElementsMatch(t, []string{"ns", "scratch"}, cleaned,
		"the cleanup of the namespace that opted out still runs, for the backups taken before")
}

func TestSchedulerRoundSkipsADeletingShardAndASplitChild(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(1, func(meta *proto.ShardMetadata) { meta.Status = proto.ShardStatusDeleting })
	f.patchShard(2, func(meta *proto.ShardMetadata) {
		meta.Split = &proto.SplitMetadata{ParentShardId: 1}
	})
	f.scheduler.round()
	assert.Empty(t, f.rpc.backups())
	assert.Nil(t, f.state(1))
	assert.Nil(t, f.state(2))
}

func TestSchedulerRoundLetsASplitChildInheritItsParentsLastBackup(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	lastBackup := f.clock.Now().Add(-20 * time.Minute)
	f.rpc.setBackup(func(string, *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
		return &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED,
			LastBackup: &proto.BackupRecord{TimestampMillis: lastBackup.UnixMilli()}}, nil
	})
	f.scheduler.round()
	require.Len(t, f.rpc.backups(), 1)
	assert.Equal(t, lastBackup, f.state(1).lastBackup)

	// The parent is gone, the children take over its range
	require.NoError(t, f.metadata.PatchNamespaceStatus("ns", func(status *proto.NamespaceStatus) bool {
		delete(status.Shards, 1)
		left := steadyShard(server1, servers...)
		left.Int32HashRange = &proto.HashRange{Min: 0, Max: 1<<31 - 1}
		right := steadyShard(server1, servers...)
		right.Int32HashRange = &proto.HashRange{Min: 1 << 31, Max: ^uint32(0)}
		status.Shards[2], status.Shards[3] = left, right
		return true
	}))
	f.scheduler.round()
	assert.Len(t, f.rpc.backups(), 1, "the children are not due before the parent's next backup")
	assert.Nil(t, f.state(1))
	assert.Equal(t, lastBackup, f.state(2).lastBackup)
	assert.Equal(t, lastBackup.Add(time.Hour), f.state(3).nextDue)

	f.clock.Advance(time.Hour)
	f.scheduler.round()
	assert.Len(t, f.rpc.backups(), 3, "both children are due an interval after the parent's backup")
}

func TestSchedulerRequestBackupForcesTheShards(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(2, func(*proto.ShardMetadata) {})
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setBackup(func(string, *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
		return &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED,
			LastBackup: &proto.BackupRecord{TimestampMillis: f.clock.Now().UnixMilli()}}, nil
	})
	f.scheduler.round()
	require.Len(t, f.rpc.backups(), 2)
	f.scheduler.round()
	require.Len(t, f.rpc.backups(), 2, "nothing is due")

	assert.Equal(t, 0, f.scheduler.RequestBackup("other", nil))
	assert.Equal(t, 1, f.scheduler.RequestBackup("ns", ptr(int64(2))))
	f.scheduler.round()
	calls := f.rpc.backups()
	require.Len(t, calls, 3)
	assert.EqualValues(t, 2, calls[2].req.Shard)
	assert.True(t, calls[2].req.Force)
	f.scheduler.round()
	assert.Len(t, f.rpc.backups(), 3, "forced once")

	assert.Equal(t, 2, f.scheduler.RequestBackup("", nil))
	select {
	case <-f.scheduler.wake:
	default:
		t.Fatal("RequestBackup wakes the loop")
	}
}

func TestSchedulerCloseForgetsTheShards(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.scheduler.round()
	require.NotNil(t, f.state(1))
	f.scheduler.Close()
	assert.Nil(t, f.state(1))
	f.scheduler.Close()
}

func TestSchedulerStartRunsRounds(t *testing.T) {
	policy := enabledPolicy()
	policy.ScheduleInterval = "10ms"
	f := newFixture(t, policy)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.scheduler.Start()
	require.Eventually(t, func() bool { return len(f.rpc.backups()) >= 2 }, 10*time.Second, time.Millisecond)
	f.scheduler.Close()
}
