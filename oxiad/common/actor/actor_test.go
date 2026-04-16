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

func TestActorPauseDrainsPendingRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan []int, 1)
	release := make(chan struct{})
	processed := make(chan []int, 2)

	a := New[int](
		ctx,
		"test-pause",
		Errors{},
		func(items []int) {
			started <- append([]int(nil), items...)
			<-release
			processed <- append([]int(nil), items...)
		},
	)
	defer func() {
		_, _ = a.Close()
	}()

	require.NoError(t, a.Add(1))
	assert.Equal(t, []int{1}, <-started)

	require.NoError(t, a.Add(2))
	require.NoError(t, a.Add(3))

	pauseErr := errors.New("metadata unavailable")
	pending, err := a.Pause()
	assert.Equal(t, []int{2, 3}, pending)
	assert.ErrorIs(t, err, pauseErr)
	assert.ErrorIs(t, a.Add(4), pauseErr)

	a.Resume()
	require.NoError(t, a.Add(5))

	close(release)

	require.Eventually(t, func() bool {
		return len(processed) == 2
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, []int{1}, <-processed)
	assert.Equal(t, []int{5}, <-processed)
}

func TestActorCloseReturnsPendingRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan []int, 1)
	release := make(chan struct{})

	shutdownErr := errors.New("metadata closed")
	a := New[int](
		ctx,
		"test-close",
		Errors{
			Shutdown: shutdownErr,
		},
		func(items []int) {
			started <- append([]int(nil), items...)
			<-release
		},
	)
	defer close(release)

	require.NoError(t, a.Add(1))
	assert.Equal(t, []int{1}, <-started)

	require.NoError(t, a.Add(2))

	pending, err := a.Close()
	assert.Equal(t, []int{2}, pending)
	assert.ErrorIs(t, err, shutdownErr)
	assert.ErrorIs(t, a.Add(3), shutdownErr)
}

func TestActorPauseWithoutErrorUsesDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan []int, 1)
	release := make(chan struct{})

	a := New[int](
		ctx,
		"test-default-pause",
		Errors{},
		func(items []int) {
			started <- append([]int(nil), items...)
			<-release
		},
	)
	defer func() {
		close(release)
		_, _ = a.Close()
	}()

	require.NoError(t, a.Add(1))
	assert.Equal(t, []int{1}, <-started)
	require.NoError(t, a.Add(2))

	pending, err := a.Pause()
	assert.Equal(t, []int{2}, pending)
	assert.ErrorIs(t, err, ErrPaused)
	assert.ErrorIs(t, a.Add(3), ErrPaused)
}

func TestActorUsesCustomPauseErrorFromErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan []int, 1)
	release := make(chan struct{})

	customPauseErr := errors.New("metadata unavailable")
	a := New[int](
		ctx,
		"test-custom-pause",
		Errors{
			Pause: customPauseErr,
		},
		func(items []int) {
			started <- append([]int(nil), items...)
			<-release
		},
	)
	defer func() {
		close(release)
		_, _ = a.Close()
	}()

	require.NoError(t, a.Add(1))
	assert.Equal(t, []int{1}, <-started)
	require.NoError(t, a.Add(2))

	pending, err := a.Pause()
	assert.Equal(t, []int{2}, pending)
	assert.ErrorIs(t, err, customPauseErr)
	assert.ErrorIs(t, a.Add(3), customPauseErr)
}
