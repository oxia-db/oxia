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

func TestSchedulerCleanupsRunOncePerCleanupInterval(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.scheduler.round()
	calls := f.rpc.expires()
	require.Len(t, calls, 1, "the first cleanup runs at the start")
	assert.Equal(t, "ns", calls[0].req.Namespace)
	assert.Equal(t, "mem://test", calls[0].req.Policy.Destination)
	assert.Equal(t, "s1", calls[0].node)

	f.clock.Advance(23 * time.Hour)
	f.scheduler.round()
	assert.Len(t, f.rpc.expires(), 1)
	f.clock.Advance(time.Hour)
	f.scheduler.round()
	calls = f.rpc.expires()
	require.Len(t, calls, 2)
	assert.Equal(t, "s2", calls[1].node, "the servers take turns")
}

func TestSchedulerCleanupsPollTheServerRunningTheCleanup(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setExpire(func(string, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
		return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_STARTED}, nil
	})
	f.scheduler.round()
	f.rpc.setExpire(func(string, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
		return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_RUNNING}, nil
	})
	f.scheduler.round()
	f.rpc.setExpire(func(string, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
		return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE, BackupsExpired: 2}, nil
	})
	f.scheduler.round()
	calls := f.rpc.expires()
	require.Len(t, calls, 3)
	for _, call := range calls {
		assert.Equal(t, "s1", call.node)
	}
	f.scheduler.round()
	assert.Len(t, f.rpc.expires(), 3, "done: the next cleanup is an interval away")
}

func TestSchedulerCleanupsTryAnotherServerWhenBusy(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setExpire(func(string, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
		return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_BUSY}, nil
	})
	f.scheduler.round()
	f.clock.Advance(30 * time.Second)
	f.scheduler.round()
	calls := f.rpc.expires()
	require.Len(t, calls, 2)
	assert.Equal(t, "s1", calls[0].node)
	assert.Equal(t, "s2", calls[1].node)
}

func TestSchedulerCleanupsBackOffAfterAFailure(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.rpc.setExpire(func(string, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
		return nil, errors.New("unavailable")
	})
	f.scheduler.round()
	f.scheduler.round()
	assert.Len(t, f.rpc.expires(), 1)
	f.clock.Advance(failureBackoff)
	f.scheduler.round()
	assert.Len(t, f.rpc.expires(), 2)
}

func TestSchedulerCleanupsSkipTheUnavailableServers(t *testing.T) {
	f := newFixture(t, enabledPolicy())
	f.down["s1"], f.down["s2"] = true, true
	f.scheduler.round()
	calls := f.rpc.expires()
	require.Len(t, calls, 1)
	assert.Equal(t, "s3", calls[0].node)
	f.down["s3"] = true
	f.clock.Advance(24 * time.Hour)
	f.scheduler.round()
	assert.Len(t, f.rpc.expires(), 1, "no server to run the cleanup")
}
