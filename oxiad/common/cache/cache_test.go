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

package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/channel"
)

const waitFor = 5 * time.Second

// source is a fake value store with a controllable watch.
type source struct {
	sync.Mutex
	value      int
	loadErrs   int
	watchErrs  int
	watchCalls int
	loads      int
	sessions   chan chan struct{}
}

func newSource(t *testing.T, value int) *source {
	t.Helper()
	return &source{value: value, sessions: make(chan chan struct{}, 16)}
}

func (s *source) set(value int) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

func (s *source) load(ctx context.Context) (*int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.Lock()
	s.loads++
	if s.loadErrs > 0 {
		s.loadErrs--
		s.Unlock()
		return nil, errors.New("load failed")
	}
	value := s.value
	s.Unlock()
	return &value, nil
}

// watch starts a watch session, which reports a change on each signal sent to
// the session channel, and ends when that channel is closed.
func (s *source) watch(ctx context.Context) (<-chan struct{}, error) {
	s.Lock()
	s.watchCalls++
	if s.watchErrs > 0 {
		s.watchErrs--
		s.Unlock()
		return nil, errors.New("watch failed")
	}
	s.Unlock()

	changes := make(chan struct{}, 1)
	signals := make(chan struct{})
	s.sessions <- signals
	go func() {
		defer close(changes)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-signals:
				if !ok {
					return
				}
				channel.PushNoBlock(changes, struct{}{})
			}
		}
	}()
	return changes, nil
}

func (s *source) loadCount() int {
	s.Lock()
	defer s.Unlock()
	return s.loads
}

func (s *source) calls() int {
	s.Lock()
	defer s.Unlock()
	return s.watchCalls
}

func nextSession(t *testing.T, s *source) chan struct{} {
	t.Helper()
	select {
	case session := <-s.sessions:
		return session
	case <-time.After(waitFor):
		t.Fatal("watch session did not start")
		return nil
	}
}

func ptr(v int) *int { return &v }

func get(t *testing.T, c *Cache[int]) int {
	t.Helper()
	value, err := c.Get()
	require.NoError(t, err)
	return *value
}

func newCache(t *testing.T, s *source) *Cache[int] {
	t.Helper()
	c := New(t.Context(), s.load, s.watch)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// newQuietCache returns a cache that has loaded the source, and loads again
// only when asked to: it has no watch.
func newQuietCache(t *testing.T, s *source) *Cache[int] {
	t.Helper()
	c := New(t.Context(), s.load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	require.Equal(t, s.value, get(t, c))
	return c
}

// The cache loads on the first Get, and a failed load is reported, without
// caching anything.
func TestCacheGetLoadsWhenEmpty(t *testing.T) {
	s := newSource(t, 1)
	s.loadErrs = 1
	c := New(t.Context(), s.load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	require.Zero(t, s.loadCount())

	_, err := c.Get()
	require.Error(t, err)
	require.Equal(t, 1, get(t, c))
	require.Equal(t, 1, get(t, c))
	require.Equal(t, 2, s.loadCount())
}

func TestCacheGetRejectsNilValue(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)
	load := func(context.Context) (*int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return nil, nil //nolint:nilnil // a load without a value, which the cache rejects
		}
		return ptr(1), nil
	}
	c := New(t.Context(), load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	_, err := c.Get()
	require.Error(t, err)
	require.Equal(t, 1, get(t, c))
}

// The loads of a closed cache get a canceled context.
func TestCacheGetFailsAfterClose(t *testing.T) {
	load := func(ctx context.Context) (*int, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return ptr(1), nil
	}
	c := New(context.Background(), load, nil)
	require.NoError(t, c.Close())
	_, err := c.Get()
	require.ErrorIs(t, err, context.Canceled)
}

func TestCacheReloadsOnWatchSignal(t *testing.T) {
	s := newSource(t, 1)
	c := newCache(t, s)
	session := nextSession(t, s)

	s.set(2)
	session <- struct{}{}
	require.Eventually(t, func() bool { return get(t, c) == 2 }, waitFor, 10*time.Millisecond)
}

func TestCacheReload(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	s.set(3)
	require.NoError(t, c.Reload())
	require.Equal(t, 3, get(t, c))
}

func TestCacheReloadRetriesUntilSuccess(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	s.Lock()
	s.value = 4
	s.loadErrs = 3
	s.Unlock()
	require.NoError(t, c.Reload())
	require.Equal(t, 4, get(t, c))
}

// Reads return the cached value while a reload keeps failing.
func TestCacheReadsDuringFailingReload(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	s.Lock()
	s.value = 2
	s.loadErrs = 1 << 20
	s.Unlock()
	reloaded := make(chan struct{})
	go func() {
		_ = c.Reload()
		close(reloaded)
	}()
	require.Eventually(t, func() bool { return s.loadCount() > 3 }, waitFor, 10*time.Millisecond)
	require.Equal(t, 1, get(t, c))

	s.Lock()
	s.loadErrs = 0
	s.Unlock()
	<-reloaded
	require.Equal(t, 2, get(t, c))
}

func TestCacheCompute(t *testing.T) {
	s := newSource(t, 1)
	c := New(t.Context(), s.load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	sub := c.Subscribe()

	// An empty cache loads the value first.
	value, err := c.Compute(func(current *int) (*int, error) {
		return ptr(*current + 1), nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, *value)
	require.Equal(t, 2, get(t, c))
	select {
	case <-sub.Changed():
	case <-time.After(waitFor):
		t.Fatal("subscriber was not notified")
	}
}

// A write that fails, maybe after being applied, empties the cache: the next
// Get loads the value.
func TestCacheComputeFailureEmptiesCache(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	writeErr := errors.New("write timed out")
	_, err := c.Compute(func(*int) (*int, error) {
		s.set(5)
		return nil, writeErr
	})
	require.ErrorIs(t, err, writeErr)
	require.Equal(t, 5, get(t, c))
}

// After a failed write, the next write loads the value first, and fails rather
// than writing with an outdated value when the load fails.
func TestCacheComputeAfterFailureLoadsFirst(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	writeErr := errors.New("write timed out")
	_, err := c.Compute(func(*int) (*int, error) {
		s.Lock()
		s.value = 5
		s.loadErrs = 1
		s.Unlock()
		return nil, writeErr
	})
	require.ErrorIs(t, err, writeErr)

	called := false
	_, err = c.Compute(func(*int) (*int, error) {
		called = true
		return ptr(6), nil
	})
	require.Error(t, err)
	require.False(t, called)

	value, err := c.Compute(func(current *int) (*int, error) {
		return ptr(*current + 1), nil
	})
	require.NoError(t, err)
	require.Equal(t, 6, *value)
}

// Reads do not wait for a write that is running.
func TestCacheGetDuringCompute(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	writing := make(chan struct{})
	release := make(chan struct{})
	computed := make(chan struct{})
	go func() {
		_, err := c.Compute(func(*int) (*int, error) {
			close(writing)
			<-release
			return ptr(2), nil
		})
		assert.NoError(t, err)
		close(computed)
	}()
	<-writing
	require.Equal(t, 1, get(t, c))

	close(release)
	<-computed
	require.Equal(t, 2, get(t, c))
}

// A load started while a write is running waits for its result, then loads
// what the source holds: a value from before the write cannot replace the
// written one, and a change made after it is not lost.
func TestCacheReloadWaitsForCompute(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	writing := make(chan struct{})
	release := make(chan struct{})
	computed := make(chan struct{})
	go func() {
		_, err := c.Compute(func(*int) (*int, error) {
			s.set(2)
			close(writing)
			<-release
			return ptr(2), nil
		})
		assert.NoError(t, err)
		close(computed)
	}()
	<-writing

	// Another writer changes the source while the write is publishing.
	s.set(3)
	reloaded := make(chan struct{})
	go func() {
		_ = c.Reload()
		close(reloaded)
	}()
	select {
	case <-reloaded:
		t.Fatal("reload ran during the write")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-computed
	<-reloaded
	require.Equal(t, 3, get(t, c))
}

func TestCacheRestartsClosedWatchAndReloads(t *testing.T) {
	s := newSource(t, 1)
	c := newCache(t, s)
	first := nextSession(t, s)

	// A change the closed session never reported is picked up by the reload
	// after the next session starts.
	s.set(7)
	close(first)
	nextSession(t, s)

	require.Eventually(t, func() bool { return get(t, c) == 7 }, waitFor, 10*time.Millisecond)
	require.Equal(t, 2, s.calls())
}

func TestCacheRetriesFailedWatch(t *testing.T) {
	s := newSource(t, 1)
	s.watchErrs = 2
	newCache(t, s)

	nextSession(t, s)
	require.Equal(t, 3, s.calls())
}

func TestCacheSubscription(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)
	first := c.Subscribe()
	second := c.Subscribe()

	s.set(8)
	require.NoError(t, c.Reload())
	for _, sub := range []*Subscription[int]{first, second} {
		select {
		case <-sub.Changed():
		case <-time.After(waitFor):
			t.Fatal("subscriber was not notified")
		}
		value, err := sub.Get()
		require.NoError(t, err)
		require.Equal(t, 8, *value)
	}

	first.Close()
	first.Close()
	_, open := <-first.Changed()
	require.False(t, open)

	_, err := c.Compute(func(*int) (*int, error) { return ptr(9), nil })
	require.NoError(t, err)
	select {
	case <-second.Changed():
	case <-time.After(waitFor):
		t.Fatal("remaining subscriber was not notified")
	}
	value, err := second.Get()
	require.NoError(t, err)
	require.Equal(t, 9, *value)
}

func TestCacheClose(t *testing.T) {
	s := newSource(t, 1)
	c := New(context.Background(), s.load, nil)
	require.Equal(t, 1, get(t, c))

	sub := c.Subscribe()
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	_, open := <-sub.Changed()
	require.False(t, open)
	_, open = <-c.Subscribe().Changed()
	require.False(t, open)
	// A reload after Close fails, and the cache keeps its value.
	require.ErrorIs(t, c.Reload(), context.Canceled)
	require.Equal(t, 1, get(t, c))
}
