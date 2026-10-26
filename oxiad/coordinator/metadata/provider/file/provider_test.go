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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
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

// A coordinator waiting for the leadership while another holds it can be
// stopped: closing its provider ends the wait with an error, and leaves the
// leader's lock in place.
func TestCloseEndsWaitToBecomeLeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")

	leader := newTestProvider(t, path)
	_, err := leader.WaitToBecomeLeader()
	require.NoError(t, err)

	waiting := newTestProvider(t, path)
	acquired := startWaitToBecomeLeader(waiting)
	requireStillWaiting(t, acquired)

	require.NoError(t, waiting.Close())
	select {
	case err := <-acquired:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the wait did not end after the provider was closed")
	}

	follower := newTestProvider(t, path)
	acquired = startWaitToBecomeLeader(follower)
	requireStillWaiting(t, acquired)

	require.NoError(t, leader.Close())
	requireBecomesLeader(t, acquired)
	require.NoError(t, follower.Close())
}

// A provider closed after its wait took the lock, but before the wait returned,
// releases the lock in the wait.
func TestCloseEndsWaitToBecomeLeaderAfterLockAcquired(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	path := filepath.Join(t.TempDir(), "metadata")
	var p statusProvider
	p, err := newProvider(t.Context(), path, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "test",
		failpoint.New(map[string]func() error{
			"waitToBecomeLeader.locked": func() error { return p.Close() },
		}))
	require.NoError(t, err)

	_, err = p.WaitToBecomeLeader()
	require.ErrorIs(t, err, context.Canceled)

	follower := newTestProvider(t, path)
	requireBecomesLeader(t, startWaitToBecomeLeader(follower))
	require.NoError(t, follower.Close())
}

// A lock that fails to be released is only logged: closing the provider still
// succeeds.
func TestCloseIgnoresUnlockFailure(t *testing.T) {
	p, err := newProvider(t.Context(), filepath.Join(t.TempDir(), "metadata"), metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled, "test", failpoint.New(map[string]func() error{
			"close.unlock": func() error { return errors.New("failpoint") },
		}))
	require.NoError(t, err)
	_, err = p.WaitToBecomeLeader()
	require.NoError(t, err)

	require.NoError(t, p.Close())
}

// A writer that rewrites the file in place, as os.WriteFile does, truncates it
// before it writes the new content, and one that replaces it may remove it
// first. A reload in between waits for the new content: an empty configuration
// would delete every namespace.
func TestProviderReloadKeepsDocumentUntilFileIsRewritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, writeConfigFile(path, 4))
	p, err := NewProvider(t.Context(), path, metadatacodec.ClusterConfigCodec, metadatacommon.WatchDisabled, "test")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, p.Close())
	})
	requireServers(t, p, 4)

	require.NoError(t, os.Truncate(path, 0))
	finishRewriteLater(t, path, 3)
	require.NoError(t, p.Reload())
	requireServers(t, p, 3)

	require.NoError(t, os.Remove(path))
	finishRewriteLater(t, path, 2)
	require.NoError(t, p.Reload())
	requireServers(t, p, 2)
}

func TestNewProviderFailsWhenParentIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))

	_, err := NewProvider(t.Context(), filepath.Join(file, "dir", "metadata"), metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled, "test")
	require.Error(t, err)
}

func TestNewProviderFailsWhenParentCannotBeCreated(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory permissions")
	}
	readOnly := filepath.Join(t.TempDir(), "read-only")
	require.NoError(t, os.Mkdir(readOnly, 0o500))

	_, err := NewProvider(t.Context(), filepath.Join(readOnly, "dir", "metadata"), metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled, "test")
	require.ErrorIs(t, err, os.ErrPermission)
}

func TestProviderGetLeaderNameReturnsName(t *testing.T) {
	p := newTestProvider(t, filepath.Join(t.TempDir(), "metadata"))
	defer func() { require.NoError(t, p.Close()) }()

	name, err := p.GetLeaderName()
	require.NoError(t, err)
	assert.Equal(t, "test", name)
}

func TestProviderLoadFailsWhenPathIsADirectory(t *testing.T) {
	p := newTestProvider(t, t.TempDir())
	defer func() { require.NoError(t, p.Close()) }()

	_, err := p.Load()
	require.Error(t, err)
}

func TestProviderStoreFailsWhenFileCannotBeRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")
	p := newTestProvider(t, path)
	defer func() { require.NoError(t, p.Close()) }()
	loaded, err := p.Load()
	require.NoError(t, err)

	// The file is replaced by something that can't be read
	require.NoError(t, os.Mkdir(path, 0o755))

	_, err = p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{},
		Version: loaded.Version,
	})
	require.Error(t, err)
}

func TestProviderStoreFailsWhenDocumentCannotBeEncoded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")
	encodeErr := errors.New("encode failed")
	p, err := NewProvider(t.Context(), path, &fakeCodec{
		Codec:       metadatacodec.ClusterStatusCodec,
		marshalYAML: func(*commonproto.ClusterStatus) ([]byte, error) { return nil, encodeErr },
	}, metadatacommon.WatchDisabled, "test")
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()

	_, err = p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{},
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, encodeErr)
	assert.NoFileExists(t, path)
}

// A document that can't be decoded must stop the coordinator, rather than be
// served or overwritten.
func TestProviderLoadPanicsOnUndecodableDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")
	require.NoError(t, os.WriteFile(path, []byte("instanceId: [unterminated"), 0o600))
	p := newTestProvider(t, path)
	defer func() { require.NoError(t, p.Close()) }()

	assert.Panics(t, func() { _, _ = p.Load() })
}

// writeConfigFile writes a configuration with the given number of servers, in
// place.
func writeConfigFile(path string, servers int) error {
	config := &commonproto.ClusterConfiguration{}
	for i := range servers {
		config.Servers = append(config.Servers, &commonproto.DataServerIdentity{
			Public:   fmt.Sprintf("server-%d:6648", i),
			Internal: fmt.Sprintf("server-%d:6649", i),
		})
	}
	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(config)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// finishRewriteLater writes a configuration with the given number of servers
// from another goroutine, after a delay: a reload that the caller starts right
// away first finds the file as the caller left it, empty or missing.
func finishRewriteLater(t *testing.T, path string, servers int) {
	t.Helper()

	written := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		written <- writeConfigFile(path, servers)
	}()
	t.Cleanup(func() {
		assert.NoError(t, <-written)
	})
}

func requireServers(t *testing.T, p provider.Provider[*commonproto.ClusterConfiguration], servers int) {
	t.Helper()

	loaded, err := p.Load()
	require.NoError(t, err)
	require.Len(t, loaded.Value.GetServers(), servers)
}

type fakeCodec struct {
	metadatacodec.Codec[*commonproto.ClusterStatus]
	marshalYAML func(value *commonproto.ClusterStatus) ([]byte, error)
}

func (c *fakeCodec) MarshalYAML(value *commonproto.ClusterStatus) ([]byte, error) {
	return c.marshalYAML(value)
}
