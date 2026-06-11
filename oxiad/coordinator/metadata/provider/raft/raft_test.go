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
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leadershipResult struct {
	lost <-chan struct{}
	err  error
}

func startWaitForLeadership(notifyCh chan bool, shutdownCh chan struct{}, barrier func() error) chan leadershipResult {
	resCh := make(chan leadershipResult, 1)
	go func() {
		lost, err := waitForLeadership(notifyCh, shutdownCh, barrier,
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
	resCh := startWaitForLeadership(notifyCh, shutdownCh, func() error {
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
	resCh := startWaitForLeadership(notifyCh, shutdownCh, func() error {
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

	resCh := startWaitForLeadership(notifyCh, shutdownCh, func() error { return nil })

	close(shutdownCh)
	res := <-resCh
	require.Error(t, res.err)
	assert.Nil(t, res.lost)
}
