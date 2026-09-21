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

package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type statusProvider = provider.Provider[*commonproto.ClusterStatus]

func newTestProvider(t *testing.T, path string) statusProvider {
	t.Helper()

	p, err := NewProvider(t.Context(), path, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "test")
	require.NoError(t, err)
	return p
}

func startWaitToBecomeLeader(p statusProvider) <-chan error {
	acquired := make(chan error, 1)
	go func() {
		_, err := p.WaitToBecomeLeader()
		acquired <- err
	}()
	return acquired
}

func requireStillWaiting(t *testing.T, acquired <-chan error) {
	t.Helper()

	select {
	case err := <-acquired:
		require.FailNow(t, "became leader while the lock was held", "err: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

func requireBecomesLeader(t *testing.T, acquired <-chan error) {
	t.Helper()

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "did not become leader after the lock was released")
	}
}

// The leadership is an exclusive lock on the metadata file, held from
// WaitToBecomeLeader until the provider is closed.
func TestWaitToBecomeLeaderIsExclusiveUntilClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")

	leader := newTestProvider(t, path)
	lost, err := leader.WaitToBecomeLeader()
	require.NoError(t, err)
	assert.Nil(t, lost)

	follower := newTestProvider(t, path)
	acquired := startWaitToBecomeLeader(follower)
	requireStillWaiting(t, acquired)

	// Storing the metadata rewrites the locked file: the lock must survive it
	version, err := leader.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{},
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	assert.EqualValues(t, "0", version)
	requireStillWaiting(t, acquired)

	require.NoError(t, leader.Close())
	requireBecomesLeader(t, acquired)
	require.NoError(t, follower.Close())
}

// Closing a provider that never became leader must not release the lock of
// the one that did.
func TestCloseWithoutLeadership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")

	leader := newTestProvider(t, path)
	_, err := leader.WaitToBecomeLeader()
	require.NoError(t, err)

	require.NoError(t, newTestProvider(t, path).Close())

	follower := newTestProvider(t, path)
	acquired := startWaitToBecomeLeader(follower)
	requireStillWaiting(t, acquired)

	require.NoError(t, leader.Close())
	requireBecomesLeader(t, acquired)
	require.NoError(t, follower.Close())
}

func TestWaitToBecomeLeaderFailsWhenLockFileCannotBeOpened(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "coordinator")
	path := filepath.Join(dir, "metadata")
	p := newTestProvider(t, path)

	// The lock file is created on demand, which requires its directory
	require.NoError(t, os.RemoveAll(dir))

	_, err := p.WaitToBecomeLeader()
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.ErrorContains(t, err, "failed to acquire lock on "+path)

	// A failed acquisition leaves nothing to release
	require.NoError(t, p.Close())
}
