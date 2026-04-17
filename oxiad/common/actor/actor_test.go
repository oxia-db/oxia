// Copyright 2023-2025 The Oxia Authors
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

package actor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActorPauseOnlyRejectsNewRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan []int, 1)
	release := make(chan struct{})
	processed := make(chan []int, 3)

	a, err := New[int](ctx, "test-pause", func(items []int) {
		batch := append([]int(nil), items...)
		started <- batch
		<-release
		processed <- batch
	}, Errors{
		Pause: errors.New("metadata unavailable"),
	})
	require.NoError(t, err)
	defer func() {
		_ = a.Close()
	}()

	require.NoError(t, a.Send(1))
	assert.Equal(t, []int{1}, <-started)

	require.NoError(t, a.Send(2))
	require.NoError(t, a.Send(3))
	require.NoError(t, a.Pause())
	assert.EqualError(t, a.Send(4), "metadata unavailable")

	close(release)

	require.Eventually(t, func() bool {
		return len(processed) == 2
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, []int{2, 3}, <-started)
	assert.Equal(t, []int{1}, <-processed)
	assert.Equal(t, []int{2, 3}, <-processed)

	require.NoError(t, a.Resume())
	release = make(chan struct{})
	require.NoError(t, a.Send(5))
	assert.Equal(t, []int{5}, <-started)
	close(release)
	assert.Equal(t, []int{5}, <-processed)
}

func TestActorPauseUsesConfiguredError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pauseErr := errors.New("metadata unavailable")
	a, err := New[int](ctx, "test-pause-err", func([]int) {}, Errors{
		Pause: pauseErr,
	})
	require.NoError(t, err)
	defer func() {
		_ = a.Close()
	}()

	require.NoError(t, a.Pause())
	assert.ErrorIs(t, a.Send(1), pauseErr)
}

func TestActorCloseRejectsNewRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	closed := make(chan error, 1)

	shutdownErr := errors.New("metadata closed")
	a, err := New[int](ctx, "test-close", func([]int) {
		started <- struct{}{}
		<-release
	}, Errors{
		Shutdown: shutdownErr,
	})
	require.NoError(t, err)

	require.NoError(t, a.Send(1))
	<-started

	go func() {
		closed <- a.Close()
	}()

	require.Eventually(t, func() bool {
		return errors.Is(a.Send(2), shutdownErr)
	}, time.Second, 10*time.Millisecond)

	assert.ErrorIs(t, a.Send(2), shutdownErr)
	close(release)
	require.NoError(t, <-closed)
}

func TestActorRequiresHandler(t *testing.T) {
	a, err := New[int](context.Background(), "test-nil", nil, Errors{})
	assert.Nil(t, a)
	assert.EqualError(t, err, "handler must not be nil")
}

func TestActorRejectsSendAfterParentContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a, err := New[int](ctx, "test-parent-cancel", func([]int) {}, Errors{})
	require.NoError(t, err)

	cancel()

	require.Eventually(t, func() bool {
		return errors.Is(a.Send(1), ErrShuttingDown)
	}, time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, a.Pause(), ErrShuttingDown)
	assert.ErrorIs(t, a.Resume(), ErrShuttingDown)
}
