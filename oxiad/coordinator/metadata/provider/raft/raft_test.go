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

package raft

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
)

// testRaftAddress binds the transport to a free port: the node ID, and so the
// data dir, is the address as given.
const testRaftAddress = "127.0.0.1:0"

func TestNewBootstrapsAndReopens(t *testing.T) {
	dataDir := t.TempDir()

	r, err := New(testRaftAddress, []string{testRaftAddress}, dataDir, nil)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close(), "close is idempotent")

	// The state is there now: the node restarts without bootstrapping again
	r, err = New(testRaftAddress, nil, dataDir, nil)
	require.NoError(t, err)
	require.NoError(t, r.Close())
}

func TestNewFailsOnUnwritableDataDir(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(dataDir, nil, 0o600))

	_, err := New(testRaftAddress, []string{testRaftAddress}, dataDir, nil)
	require.ErrorContains(t, err, "failed to create data dir")
}

func TestNewFailsOnInvalidAddress(t *testing.T) {
	_, err := New("invalid-address", nil, t.TempDir(), nil)
	require.ErrorContains(t, err, "failed to resolve raft address")
}

func TestNewFailsWhenAddressInUse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	address := listener.Addr().String()

	_, err = New(address, []string{address}, t.TempDir(), nil)
	require.ErrorContains(t, err, "failed to create raft transport")
}

func TestNewFailsOnStoreError(t *testing.T) {
	dataDir := t.TempDir()
	nodeDir := filepath.Join(dataDir, testRaftAddress)
	require.NoError(t, os.MkdirAll(nodeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nodeDir, "store"), nil, 0o600))

	_, err := New(testRaftAddress, []string{testRaftAddress}, dataDir, nil)
	require.ErrorContains(t, err, "failed to create data store")
}

func TestNewFailsOnSnapshotStoreError(t *testing.T) {
	dataDir := t.TempDir()
	nodeDir := filepath.Join(dataDir, testRaftAddress)
	require.NoError(t, os.MkdirAll(nodeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nodeDir, "snapshots"), nil, 0o600))

	_, err := New(testRaftAddress, []string{testRaftAddress}, dataDir, nil)
	require.ErrorContains(t, err, "failed to create snapshot store")
}

func TestNewFailsOnCorruptLog(t *testing.T) {
	dataDir := t.TempDir()
	store, err := newKVRaftStore(filepath.Join(dataDir, testRaftAddress, "store"))
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte("log-00000000000000000001"), []byte("not json")))
	require.NoError(t, store.Close())

	_, err = New(testRaftAddress, []string{testRaftAddress}, dataDir, nil)
	require.ErrorContains(t, err, "failed to create raft node")
}

func TestNewFailsToCheckExistingState(t *testing.T) {
	skipWithoutFailpoints(t)
	dataDir := t.TempDir()
	failpointErr := errors.New("failpoint")
	_, err := newRaft(testRaftAddress, []string{testRaftAddress}, dataDir, nil, failpoint.New(map[string]func() error{
		"new.hasExistingState": func() error { return failpointErr },
	}))
	require.ErrorIs(t, err, failpointErr)

	// The failed start released the address and the data dir
	r, err := New(testRaftAddress, []string{testRaftAddress}, dataDir, nil)
	require.NoError(t, err)
	require.NoError(t, r.Close())
}

func TestNewFailsToBootstrapWithoutNodes(t *testing.T) {
	_, err := New(testRaftAddress, nil, t.TempDir(), nil)
	require.ErrorContains(t, err, "failed to create raft node")
}

type leadershipResult struct {
	lost <-chan struct{}
	err  error
}

func startWaitForLeadership(ctx context.Context, notifyCh chan bool, shutdownCh chan struct{},
	barrier func() error) chan leadershipResult {
	resCh := make(chan leadershipResult, 1)
	go func() {
		lost, err := waitForLeadership(ctx, notifyCh, shutdownCh, barrier,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		resCh <- leadershipResult{lost, err}
	}()
	return resCh
}

func TestWaitForLeadership(t *testing.T) {
	notifyCh := make(chan bool, 1)
	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	barriers := atomic.Int32{}
	resCh := startWaitForLeadership(t.Context(), notifyCh, shutdownCh, func() error {
		barriers.Add(1)
		return nil
	})

	// Loss notifications while waiting must not be mistaken for a win
	notifyCh <- false
	notifyCh <- false
	notifyCh <- true

	res := <-resCh
	require.NoError(t, res.err)
	require.NotNil(t, res.lost)
	assert.EqualValues(t, 1, barriers.Load())

	// Still leading: the lost channel stays open
	select {
	case <-res.lost:
		t.Fatal("lost channel closed while still leading")
	case <-time.After(100 * time.Millisecond):
	}

	// Losing the leadership closes the channel
	notifyCh <- false
	select {
	case <-res.lost:
	case <-time.After(10 * time.Second):
		t.Fatal("lost channel not closed on leadership loss")
	}

	// The notifications must keep being consumed after the loss: raft blocks
	// writing to the channel otherwise
	for i := 0; i < 5; i++ {
		select {
		case notifyCh <- i%2 == 0:
		case <-time.After(10 * time.Second):
			t.Fatal("notification channel not drained after the loss")
		}
	}
}

func TestWaitForLeadershipBarrierFailure(t *testing.T) {
	notifyCh := make(chan bool, 1)
	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	// The leadership can be lost again while waiting for the barrier: the
	// wait must resume instead of serving a stale takeover
	barriers := atomic.Int32{}
	resCh := startWaitForLeadership(t.Context(), notifyCh, shutdownCh, func() error {
		if barriers.Add(1) == 1 {
			return errors.New("leadership lost during the barrier")
		}
		return nil
	})

	notifyCh <- true
	select {
	case res := <-resCh:
		t.Fatalf("returned after a failed barrier: %+v", res)
	case <-time.After(100 * time.Millisecond):
	}

	notifyCh <- true
	res := <-resCh
	require.NoError(t, res.err)
	require.NotNil(t, res.lost)
	assert.EqualValues(t, 2, barriers.Load())
}

func TestWaitForLeadershipShutdown(t *testing.T) {
	notifyCh := make(chan bool, 1)
	shutdownCh := make(chan struct{})

	resCh := startWaitForLeadership(t.Context(), notifyCh, shutdownCh, func() error { return nil })

	close(shutdownCh)
	res := <-resCh
	require.Error(t, res.err)
	assert.Nil(t, res.lost)
}

// A canceled wait returns the context's error, and leaves a consumer on the
// notification channel, which raft blocks writing to.
func TestWaitForLeadershipCanceled(t *testing.T) {
	notifyCh := make(chan bool)
	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	ctx, cancel := context.WithCancel(t.Context())

	resCh := startWaitForLeadership(ctx, notifyCh, shutdownCh, func() error { return nil })

	cancel()
	res := <-resCh
	require.ErrorIs(t, res.err, context.Canceled)
	assert.Nil(t, res.lost)
	select {
	case notifyCh <- true:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no consumer is left on the notification channel")
	}
}
