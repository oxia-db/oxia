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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSchedulerDispatchAssignsAFollowerAsBackupReplica(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()

	assert.Equal(t, "s2", f.shard(1).GetBackupReplica().GetNameOrDefault(), "the first follower by name")
	calls := f.rpc.backups()
	require.Len(t, calls, 1)
	assert.Equal(t, "s2", calls[0].node)
	req := calls[0].req
	assert.Equal(t, "ns", req.Namespace)
	assert.EqualValues(t, 1, req.Shard)
	assert.False(t, req.Force)
	assert.Equal(t, "mem://test", req.Policy.Destination)
	assert.Equal(t, time.Hour.Milliseconds(), req.Policy.IntervalMillis)
	assert.Equal(t, (7 * 24 * time.Hour).Milliseconds(), req.Policy.RetentionMillis)
	assert.EqualValues(t, 2, req.Policy.MaxConcurrentPerServer)
	assert.Equal(t, "ns", req.NamespaceConfig.GetName())
	assert.EqualValues(t, 3, req.NamespaceConfig.GetReplicationFactor())
	assert.Equal(t, f.shard(1).GetInt32HashRange(), req.HashRange)
}

func TestSchedulerDispatchSpreadsTheRolesAcrossTheServers(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(2, func(*proto.ShardMetadata) {})
	for _, server := range []string{"s1", "s2", "s3"} {
		f.rpc.setStatus(server, proto.ServingStatus_FOLLOWER, 10)
	}
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.scheduler.round()
	replicas := map[string]int{}
	for _, shard := range []int64{1, 2} {
		replicas[f.shard(shard).GetBackupReplica().GetNameOrDefault()]++
	}
	assert.Equal(t, map[string]int{"s2": 1, "s3": 1}, replicas)
}

func TestSchedulerDispatchFallsBackToTheLeaderWhenNoFollowerIsAvailable(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.down["s2"], f.down["s3"] = true, true
	f.scheduler.round()
	assert.Equal(t, "s1", f.shard(1).GetBackupReplica().GetNameOrDefault())
	calls := f.rpc.backups()
	require.Len(t, calls, 1)
	assert.Equal(t, "s1", calls[0].node, "the leader needs no status check")
}

func TestSchedulerDispatchWaitsWhenNoMemberIsAvailable(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	for _, server := range []string{"s1", "s2", "s3"} {
		f.down[server] = true
	}
	f.scheduler.round()
	assert.Nil(t, f.shard(1).GetBackupReplica())
	assert.Empty(t, f.rpc.backups())
}

func TestSchedulerDispatchKeepsTheReplicaAcrossRounds(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(1, func(meta *proto.ShardMetadata) { meta.BackupReplica = server3 })
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()
	assert.Equal(t, "s3", f.shard(1).GetBackupReplica().GetNameOrDefault())
	calls := f.rpc.backups()
	require.Len(t, calls, 1)
	assert.Equal(t, "s3", calls[0].node)
}

func TestSchedulerDispatchReassignsAReplicaThatLeftTheEnsemble(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(1, func(meta *proto.ShardMetadata) {
		meta.BackupReplica = server3
		meta.Ensemble = []*proto.DataServerIdentity{server1, server2}
	})
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()
	assert.Equal(t, "s2", f.shard(1).GetBackupReplica().GetNameOrDefault())
}

func TestSchedulerDispatchHoldsAFollowerThatIsNotCaughtUp(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 9)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()
	assert.Equal(t, "s2", f.shard(1).GetBackupReplica().GetNameOrDefault())
	assert.Empty(t, f.rpc.backups(), "s2 is behind the leader")
	assert.Equal(t, f.clock.Now(), f.state(1).unavailableSince)

	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()
	assert.Len(t, f.rpc.backups(), 1)
	assert.True(t, f.state(1).unavailableSince.IsZero())
}

func TestSchedulerDispatchMovesTheReplicaAfterHalfAnInterval(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.statusErrs["s2"] = errors.New("connection refused")
	f.scheduler.round()
	assert.Equal(t, "s2", f.shard(1).GetBackupReplica().GetNameOrDefault())
	assert.Empty(t, f.rpc.backups())

	f.clock.Advance(29 * time.Minute)
	f.scheduler.round()
	assert.Equal(t, "s2", f.shard(1).GetBackupReplica().GetNameOrDefault(), "not yet")
	f.clock.Advance(time.Minute)
	f.scheduler.round()
	assert.Equal(t, "s3", f.shard(1).GetBackupReplica().GetNameOrDefault(), "moved to the other follower")
	assert.True(t, f.state(1).unavailableSince.IsZero())
	f.scheduler.round()
	calls := f.rpc.backups()
	require.Len(t, calls, 1)
	assert.Equal(t, "s3", calls[0].node)
}

func TestSchedulerDispatchMovesTheReplicaOfAnUnavailableServer(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.patchShard(1, func(meta *proto.ShardMetadata) { meta.BackupReplica = server2 })
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.down["s2"] = true
	f.scheduler.round()
	assert.Empty(t, f.rpc.backups())
	f.clock.Advance(30 * time.Minute)
	f.scheduler.round()
	assert.Equal(t, "s3", f.shard(1).GetBackupReplica().GetNameOrDefault())
}

func TestSchedulerApplyResponseSchedulesTheNextRequest(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	now := f.clock.Now()
	record := &proto.BackupRecord{BackupId: "id", TimestampMillis: now.Add(-10 * time.Minute).UnixMilli()}
	for _, tt := range []struct {
		name     string
		response *proto.BackupShardResponse
		err      error
		nextDue  time.Time
		failures int
	}{
		{name: "not needed, fresh", nextDue: now.Add(50 * time.Minute),
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED, LastBackup: record}},
		{name: "not needed, unchanged", nextDue: now.Add(time.Hour),
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED,
				LastBackup: &proto.BackupRecord{TimestampMillis: now.Add(-3 * time.Hour).UnixMilli()}}},
		{name: "started", nextDue: now,
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_STARTED}},
		{name: "running", nextDue: now,
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_RUNNING}},
		{name: "busy", nextDue: now,
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_BUSY}},
		{name: "done", nextDue: now.Add(50 * time.Minute),
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE, LastBackup: record}},
		{name: "failed", nextDue: now.Add(failureBackoff), failures: 1,
			response: &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_FAILED, Error: "boom"}},
		{name: "error", nextDue: now.Add(failureBackoff), failures: 1, err: errors.New("unavailable")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := &shardState{key: shardKey{namespace: "ns", shard: 1}, forced: true}
			f.scheduler.applyResponse(state, tt.response, tt.err, enabledPolicy(), now, f.scheduler.log)
			assert.Equal(t, tt.nextDue, state.nextDue)
			assert.Equal(t, tt.failures, state.failures)
			if tt.response != nil && tt.response.LastBackup != nil {
				assert.Equal(t, time.UnixMilli(tt.response.LastBackup.TimestampMillis).UTC(), state.lastBackup)
			}
		})
	}
}

func TestSchedulerApplyResponseBacksOffUpToTheInterval(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	state := &shardState{key: shardKey{namespace: "ns", shard: 1}}
	now := f.clock.Now()
	var delays []time.Duration
	for range 12 {
		f.scheduler.applyResponse(state, nil, errors.New("down"), enabledPolicy(), now, f.scheduler.log)
		delays = append(delays, state.nextDue.Sub(now))
	}
	assert.Equal(t, failureBackoff, delays[0])
	assert.Equal(t, 2*failureBackoff, delays[1])
	assert.Equal(t, time.Hour, delays[11], "capped at the interval")
}

func TestSchedulerDispatchHandlesManyShardsAtOnce(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	for shard := int64(2); shard <= 20; shard++ {
		f.patchShard(shard, func(*proto.ShardMetadata) {})
	}
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.rpc.setStatus("s3", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.round()
	assert.Len(t, f.rpc.backups(), 20)
}

func TestSchedulerDispatchAcceptsAFencedFollowerAtTheLeadersCommitOffset(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FENCED, 10)
	f.scheduler.round()
	calls := f.rpc.backups()
	require.Len(t, calls, 1)
	assert.Equal(t, "s2", calls[0].node)

	f.rpc.setStatus("s2", proto.ServingStatus_NOT_MEMBER, 10)
	f.clock.Advance(time.Hour)
	f.scheduler.round()
	assert.Len(t, f.rpc.backups(), 1, "a member that doesn't host the shard isn't ready")
}

func TestSchedulerDispatchStopsOnceClosed(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setStatus("s1", proto.ServingStatus_LEADER, 10)
	f.rpc.setStatus("s2", proto.ServingStatus_FOLLOWER, 10)
	f.scheduler.Close()
	f.scheduler.round()
	assert.Empty(t, f.rpc.backups(), "a closed scheduler sends no request")
}

func TestPreferFollowersOrdersTheLeaderLast(t *testing.T) {
	roles := &roleCounts{counts: map[string]int{}}
	order := preferFollowers("s1", roles)
	assert.Positive(t, order(server1, server2))
	assert.Negative(t, order(server2, server1))
	assert.Negative(t, order(server2, server3), "then by name")
	roles.counts["s2"] = 2
	assert.Positive(t, order(server2, server3), "then by the roles held")
}

func TestSchedulerAssignReplicaReportsWhatItCouldNotStore(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	state := &shardState{key: shardKey{namespace: "ns", shard: 1}}
	roles := &roleCounts{counts: map[string]int{}}
	meta := f.shard(1)
	// A member that left the ensemble meanwhile is not stored
	stranger := &proto.DataServerIdentity{Name: ptr("s9"), Public: "s9:6648", Internal: "s9:6649"}
	assert.True(t, f.scheduler.assignReplica(state, meta, stranger, roles, f.scheduler.log))
	assert.Nil(t, f.shard(1).GetBackupReplica())
	// A metadata that fails to store it
	s := f.withFailingMetadata(&failingMetadata{patchStatus: errors.New("metadata unavailable")})
	assert.False(t, s.assignReplica(state, meta, server2, roles, s.log))
}

func TestSchedulerApplyResponseFailsOnAnUnknownState(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	state := &shardState{key: shardKey{namespace: "ns", shard: 1}}
	now := f.clock.Now()
	f.scheduler.applyResponse(state, &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_UNKNOWN},
		nil, enabledPolicy(), now, f.scheduler.log)
	assert.Equal(t, 1, state.failures)
	assert.Equal(t, now.Add(failureBackoff), state.nextDue)
}

func TestSchedulerEnsureReplicaGivesUpWhenTheAssignmentCannotBeStored(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	s := f.withFailingMetadata(&failingMetadata{patchStatus: errors.New("metadata unavailable")})
	state := &shardState{key: shardKey{namespace: "ns", shard: 1}}
	roles := &roleCounts{counts: map[string]int{}}
	assert.Nil(t, s.ensureReplica(state, f.shard(1), roles, s.log))
}
