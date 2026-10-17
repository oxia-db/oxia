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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

const eventually = 10 * time.Second

func newTestExecutor(t *testing.T, host *fakeHost, clock *testClock) (*Executor, string, *blob.Memory) {
	t.Helper()
	destination := "mem://" + t.Name()
	store := blob.SharedMemory(t.Name())
	store.SetClock(clock.Now)
	e := New(Options{Host: host, Clock: clock.Now})
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	return e, destination, store
}

func backupRequest(destination string, shard int64) *proto.BackupShardRequest {
	return &proto.BackupShardRequest{
		Namespace:       "ns",
		Shard:           shard,
		Policy:          protoPolicy(destination),
		NamespaceConfig: &proto.Namespace{Name: "ns", ReplicationFactor: 3},
		HashRange:       fullRange(),
	}
}

// awaitBackup sends the request until the job it started ends, and returns
// the response that reports its end.
func awaitBackup(t *testing.T, e *Executor, req *proto.BackupShardRequest) *proto.BackupShardResponse {
	t.Helper()
	var res *proto.BackupShardResponse
	require.Eventually(t, func() bool {
		var err error
		res, err = e.BackupShard(t.Context(), req)
		require.NoError(t, err)
		return res.State != proto.BackupJobState_BACKUP_JOB_STATE_RUNNING &&
			res.State != proto.BackupJobState_BACKUP_JOB_STATE_STARTED
	}, eventually, time.Millisecond)
	return res
}

func TestExecutorBackupShardTakesABackupThenReportsItOnce(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	clock := newTestClock()
	e, destination, store := newTestExecutor(t, host, clock)
	req := backupRequest(destination, 1)

	res, err := e.BackupShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	assert.Nil(t, res.LastBackup)

	res = awaitBackup(t, e, req)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, res.State)
	require.NotNil(t, res.LastBackup)
	assert.Equal(t, clock.Now().UnixMilli(), res.LastBackup.TimestampMillis)
	assert.EqualValues(t, 5, res.LastBackup.CommitOffset)
	assert.Len(t, res.LastBackup.Lineage, 16)
	assert.NotEmpty(t, store.Keys())
	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/"+res.LastBackup.BackupId+".json")
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"ns","replicationFactor":3}`, string(m.NamespaceConfig))

	res, err = e.BackupShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED, res.State, "the policy is satisfied")
	assert.Equal(t, m.ID, res.LastBackup.BackupId)
}

func TestExecutorBackupShardFollowsThePolicyAcrossRequests(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	req := backupRequest(destination, 1)
	first := awaitBackup(t, e, req)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, first.State)

	clock.Advance(2 * time.Hour)
	res, err := e.BackupShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED, res.State, "unchanged since the last backup")

	host.write(1, "000002.sst", []byte("more"))
	second := awaitBackup(t, e, req)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, second.State)
	assert.NotEqual(t, first.LastBackup.BackupId, second.LastBackup.BackupId)
	assert.Equal(t, first.LastBackup.Lineage, second.LastBackup.Lineage)
	assert.Less(t, second.LastBackup.UploadedBytes, second.LastBackup.SizeBytes, "incremental")

	forced := backupRequest(destination, 1)
	forced.Force = true
	third := awaitBackup(t, e, forced)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, third.State)
	assert.NotEqual(t, second.LastBackup.BackupId, third.LastBackup.BackupId)
}

func TestExecutorBackupShardStartsANewLineageWhenTheRecordIsGone(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	req := backupRequest(destination, 1)
	first := awaitBackup(t, e, req)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, first.State)

	// A snapshot install replaces the database directory, record included
	host.shards[1].dir = t.TempDir()
	clock.Advance(time.Hour)
	second := awaitBackup(t, e, req)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, second.State)
	assert.NotEqual(t, first.LastBackup.Lineage, second.LastBackup.Lineage)
	assert.Equal(t, second.LastBackup.SizeBytes, second.LastBackup.UploadedBytes, "a full upload")
}

func TestExecutorBackupShardReportsAFailureOnce(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	clock := newTestClock()
	e, destination, store := newTestExecutor(t, host, clock)
	store.FailPut(func(string) error { return errors.New("bucket unavailable") })
	req := backupRequest(destination, 1)

	res := awaitBackup(t, e, req)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_FAILED, res.State)
	assert.Contains(t, res.Error, "bucket unavailable")
	assert.Nil(t, res.LastBackup)

	store.FailPut(nil)
	res, err := e.BackupShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State, "the next request starts again")
	res = awaitBackup(t, e, req)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, res.State)
}

func TestExecutorBackupShardIsBusyAtTheConcurrencyLimit(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	host.addShard(2, sstFiles(1))
	host.checkpointGate = make(chan struct{})
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	first := backupRequest(destination, 1)
	first.Policy.MaxConcurrentPerServer = 1
	second := backupRequest(destination, 2)
	second.Policy.MaxConcurrentPerServer = 1

	res, err := e.BackupShard(t.Context(), first)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	res, err = e.BackupShard(t.Context(), first)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_RUNNING, res.State)
	res, err = e.BackupShard(t.Context(), second)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_BUSY, res.State)

	close(host.checkpointGate)
	res = awaitBackup(t, e, first)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, res.State)
	res = awaitBackup(t, e, second)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, res.State)
}

func TestExecutorBackupShardRejectsInvalidRequests(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	e, destination, _ := newTestExecutor(t, host, newTestClock())

	_, err := e.BackupShard(t.Context(), backupRequest("", 1))
	assert.ErrorContains(t, err, "no destination")
	req := backupRequest(destination, 1)
	req.Policy.RetentionMillis = req.Policy.IntervalMillis
	_, err = e.BackupShard(t.Context(), req)
	assert.ErrorContains(t, err, "retention longer than its interval")
	_, err = e.BackupShard(t.Context(), backupRequest(destination, 99))
	assert.ErrorIs(t, err, errNotHosted)
	// A request without a namespace must not reach the host, whose paths
	// need one
	noNamespace := backupRequest(destination, 1)
	noNamespace.Namespace = ""
	_, err = e.BackupShard(t.Context(), noNamespace)
	assert.ErrorContains(t, err, "needs a namespace")
	_, err = e.ListBackups(t.Context(), &proto.ListBackupsRequest{Destination: destination})
	assert.ErrorContains(t, err, "no namespace")
}

func TestExecutorStartRunsOneJobAtATimePerKey(t *testing.T) {
	e := New(Options{Host: newFakeHost(t)})
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	key := jobKey{kind: backupJobKind, namespace: "ns", shard: 1}
	release := make(chan struct{})
	parked := func(context.Context) (any, error) {
		<-release
		return "done", nil
	}
	// Two requests that both found no job at the key: the second one
	// reports the job of the first instead of starting another
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, e.start(key, 2, parked))
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_RUNNING, e.start(key, 2, parked))
	other := jobKey{kind: backupJobKind, namespace: "ns", shard: 2}
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, e.start(other, 2, parked), "another key is free")
	close(release)
	require.Eventually(t, func() bool {
		state, _ := e.consume(key)
		return state == proto.BackupJobState_BACKUP_JOB_STATE_DONE
	}, eventually, time.Millisecond)
}

func TestExecutorBackupShardUploadsEverythingToANewDestination(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, awaitBackup(t, e, backupRequest(destination, 1)).State)

	// The policy moves to another destination: the record of the first one
	// is no backup there, and nothing of it is shared
	other := blob.SharedMemory(t.Name() + "-other")
	other.SetClock(clock.Now)
	req := backupRequest(destination+"-other", 1)
	res, err := e.BackupShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	assert.Nil(t, res.LastBackup)
	done := awaitBackup(t, e, req)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, done.State)
	ssts := 0
	for _, key := range other.Keys() {
		if backup.IsSSTName(key[strings.LastIndex(key, "/")+1:]) {
			ssts++
		}
	}
	assert.Equal(t, 2, ssts, "every SST is at the new destination")
}

func TestExecutorBackupShardKeepsNoRecordWhenTheDatabaseIsReplacedDuringTheBackup(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	host.publishGate = make(chan struct{})
	clock := newTestClock()
	e, destination, store := newTestExecutor(t, host, clock)
	req := backupRequest(destination, 1)
	res, err := e.BackupShard(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	// The backup is uploaded and about to publish its record: a snapshot
	// install replaces the database first
	require.Eventually(t, func() bool {
		host.mu.Lock()
		defer host.mu.Unlock()
		return host.withDatabaseCalls == 2
	}, eventually, time.Millisecond)
	host.replace(1)
	close(host.publishGate)

	done := awaitBackup(t, e, req)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, done.State, "the backup of the old database is valid")
	assert.Len(t, manifestKeys(store), 1)
	assert.Nil(t, done.LastBackup, "the new database has no record of it")
	_, found, err := readRecord(host.DatabaseDir("ns", 1))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestExecutorExpireBackupsRunsACleanupAndReportsItsCounts(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	clock := newTestClock()
	e, destination, store := newTestExecutor(t, host, clock)
	req := backupRequest(destination, 1)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, awaitBackup(t, e, req).State)
	// An orphan older than the grace period
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/sst/orphan-000001.sst", strings.NewReader("")))
	store.Touch("ns/shard-1/sst/orphan-000001.sst", clock.Now().Add(-2*DefaultGracePeriod))

	expire := &proto.ExpireBackupsRequest{Namespace: "ns", Policy: protoPolicy(destination)}
	res, err := e.ExpireBackups(t.Context(), expire)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	require.Eventually(t, func() bool {
		res, err = e.ExpireBackups(t.Context(), expire)
		require.NoError(t, err)
		return res.State != proto.BackupJobState_BACKUP_JOB_STATE_RUNNING
	}, eventually, time.Millisecond)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, res.State)
	assert.EqualValues(t, 0, res.BackupsExpired)
	assert.EqualValues(t, 1, res.FilesDeleted)
	assert.NotContains(t, store.Keys(), "ns/shard-1/sst/orphan-000001.sst")

	_, err = e.ExpireBackups(t.Context(), &proto.ExpireBackupsRequest{Policy: protoPolicy(destination)})
	assert.ErrorContains(t, err, "no namespace")
}

func TestExecutorListBackupsReturnsTheBackupsOfANamespace(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	done := awaitBackup(t, e, backupRequest(destination, 1))
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, done.State)

	res, err := e.ListBackups(t.Context(), &proto.ListBackupsRequest{Destination: destination, Namespace: "ns"})
	require.NoError(t, err)
	require.Len(t, res.Backups, 1)
	info := res.Backups[0]
	assert.EqualValues(t, 1, info.Shard)
	assert.Equal(t, done.LastBackup.BackupId, info.BackupId)
	assert.Equal(t, done.LastBackup.TimestampMillis, info.TimestampMillis)
	assert.Equal(t, done.LastBackup.SizeBytes, info.SizeBytes)
	assert.EqualValues(t, 1, info.SstCount)
	assert.Equal(t, fullRange(), info.HashRange)
	assert.Equal(t, "ns", info.NamespaceConfig.GetName())
	assert.EqualValues(t, 3, info.NamespaceConfig.GetReplicationFactor())

	_, err = e.ListBackups(t.Context(), &proto.ListBackupsRequest{Namespace: "ns"})
	assert.ErrorContains(t, err, "no destination")
}

func TestExecutorRestoreShardInstallsABackupAndReportsItOnce(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	done := awaitBackup(t, e, backupRequest(destination, 1))
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, done.State)

	req := &proto.RestoreShardRequest{
		Namespace:       "restored",
		Shard:           7,
		HashRange:       fullRange(),
		Destination:     destination,
		SourceNamespace: "ns",
		SourceShard:     1,
		BackupId:        done.LastBackup.BackupId,
	}
	res, err := e.RestoreShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	require.Eventually(t, func() bool {
		res, err = e.RestoreShard(t.Context(), req)
		require.NoError(t, err)
		return res.State != proto.BackupJobState_BACKUP_JOB_STATE_RUNNING
	}, eventually, time.Millisecond)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_DONE, res.State)
	assert.EqualValues(t, 42, res.CommitOffset)
	assert.EqualValues(t, 3, res.Term)
	installer := host.installer(7)
	require.NotNil(t, installer)
	assert.Equal(t, host.shards[1].files, installer.files)
	assert.True(t, installer.completed)

	res, err = e.RestoreShard(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State, "reported once, then installed again")
}

func TestExecutorRestoreShardReportsAFailure(t *testing.T) {
	host := newFakeHost(t)
	host.installerErr = errors.New("shard is led here")
	clock := newTestClock()
	e, destination, _ := newTestExecutor(t, host, clock)
	req := &proto.RestoreShardRequest{Namespace: "restored", Shard: 7, HashRange: fullRange(), Destination: destination,
		SourceNamespace: "ns", SourceShard: 1, BackupId: "2026-10-03T14-00-00Z-abcd"}
	res, err := e.RestoreShard(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	require.Eventually(t, func() bool {
		res, err = e.RestoreShard(t.Context(), req)
		require.NoError(t, err)
		return res.State != proto.BackupJobState_BACKUP_JOB_STATE_RUNNING
	}, eventually, time.Millisecond)
	assert.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_FAILED, res.State)
	assert.Contains(t, res.Error, "shard is led here")

	_, err = e.RestoreShard(t.Context(), &proto.RestoreShardRequest{Namespace: "restored", Shard: 7, BackupId: "garbage"})
	assert.ErrorIs(t, err, backup.ErrInvalidID)
	_, err = e.RestoreShard(t.Context(), &proto.RestoreShardRequest{BackupId: "garbage"})
	assert.ErrorContains(t, err, "needs a namespace")
}

func TestExecutorCloseWaitsForTheRunningJobs(t *testing.T) {
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	host.checkpointGate = make(chan struct{})
	e := New(Options{Host: host})
	res, err := e.BackupShard(t.Context(), backupRequest("mem://"+t.Name(), 1))
	require.NoError(t, err)
	require.Equal(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State)
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while a job was running")
	case <-time.After(50 * time.Millisecond):
	}
	close(host.checkpointGate)
	require.NoError(t, <-closed)
	res, err = e.BackupShard(t.Context(), backupRequest("mem://"+t.Name(), 1))
	require.NoError(t, err)
	assert.NotEqual(t, proto.BackupJobState_BACKUP_JOB_STATE_STARTED, res.State, "no job starts after Close")
}
