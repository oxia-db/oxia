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
	}, StatusActive)
	require.NoError(t, err)
	defer func() {
		_ = a.Close()
	}()

	require.NoError(t, a.Send(1))
	assert.Equal(t, []int{1}, <-started)

	require.NoError(t, a.Send(2))
	require.NoError(t, a.Send(3))
	require.NoError(t, a.Pause())
	assert.ErrorIs(t, a.Send(4), ErrPaused)

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

func TestActorCanStartPaused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan []int, 1)
	a, err := New[int](ctx, "test-start-paused", func(items []int) {
		started <- append([]int(nil), items...)
	}, StatusPaused)
	require.NoError(t, err)
	defer func() {
		_ = a.Close()
	}()

	assert.ErrorIs(t, a.Send(1), ErrPaused)
	require.NoError(t, a.Resume())
	require.NoError(t, a.Send(2))
	assert.Equal(t, []int{2}, <-started)
}

func TestActorCloseRejectsNewRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	closed := make(chan error, 1)

	a, err := New[int](ctx, "test-close", func([]int) {
		started <- struct{}{}
		<-release
	}, StatusActive)
	require.NoError(t, err)

	require.NoError(t, a.Send(1))
	<-started

	go func() {
		closed <- a.Close()
	}()

	require.Eventually(t, func() bool {
		return errors.Is(a.Send(2), ErrShuttingDown)
	}, time.Second, 10*time.Millisecond)

	assert.ErrorIs(t, a.Send(2), ErrShuttingDown)
	close(release)
	require.NoError(t, <-closed)
}

func TestActorRejectsSendAfterParentContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a, err := New[int](ctx, "test-parent-cancel", func([]int) {}, StatusActive)
	require.NoError(t, err)

	cancel()

	require.Eventually(t, func() bool {
		return errors.Is(a.Send(1), ErrShuttingDown)
	}, time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, a.Pause(), ErrShuttingDown)
	assert.ErrorIs(t, a.Resume(), ErrShuttingDown)
}
