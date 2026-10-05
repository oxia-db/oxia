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
	"sync/atomic"
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

func (s *source) load(context.Context) (*int, error) {
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

// cached returns the cached value without loading it.
func cached(c *Cache[int]) *int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// get returns the cached value without loading, or -1 when the cache has
// none.
func get(c *Cache[int]) int {
	if v := cached(c); v != nil {
		return *v
	}
	return -1
}

func newCache(t *testing.T, s *source) *Cache[int] {
	t.Helper()
	c := New(t.Context(), s.load, s.watch)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// newQuietCache returns a cache that has loaded the source and loads again
// only when asked to: it has no watch.
func newQuietCache(t *testing.T, s *source) *Cache[int] {
	t.Helper()
	c := New(t.Context(), s.load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	_, err := c.Reload()
	require.NoError(t, err)
	return c
}

func TestCacheInitialLoad(t *testing.T) {
	s := newSource(t, 1)
	s.loadErrs = 2
	c := newCache(t, s)
	require.Eventually(t, func() bool { return get(c) == 1 }, waitFor, 10*time.Millisecond)
}

func TestCacheGetWaitsForValue(t *testing.T) {
	s := newSource(t, 1)
	s.Lock()
	s.loadErrs = 3
	s.Unlock()
	c := newCache(t, s)
	require.Equal(t, 1, *c.Get())

	// After Invalidate, Get waits for the reload instead of returning nil.
	s.Lock()
	s.value = 2
	s.loadErrs = 2
	s.Unlock()
	c.Invalidate()
	require.Equal(t, 2, *c.Get())
}

func TestCacheGetAfterCloseWhileEmpty(t *testing.T) {
	s := newSource(t, 1)
	s.Lock()
	s.loadErrs = 1 << 20
	s.Unlock()
	c := New(context.Background(), s.load, s.watch)
	require.NoError(t, c.Close())
	require.Nil(t, c.Get())
}

func TestCacheRetriesNilValue(t *testing.T) {
	calls := 0
	var mu sync.Mutex
	load := func(context.Context) (*int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls < 3 {
			return nil, nil //nolint:nilnil // a load without a value, which the cache retries
		}
		return ptr(1), nil
	}
	c := New(t.Context(), load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	value, err := c.Reload()
	require.NoError(t, err)
	require.Equal(t, 1, *value)
}

func TestCacheSetNilPanics(t *testing.T) {
	c := newQuietCache(t, newSource(t, 1))
	require.Panics(t, func() { c.Set(nil) })
	require.Equal(t, 1, get(c))
}

func TestCacheReloadsOnWatchSignal(t *testing.T) {
	s := newSource(t, 1)
	c := newCache(t, s)
	session := nextSession(t, s)

	s.set(2)
	session <- struct{}{}
	require.Eventually(t, func() bool { return get(c) == 2 }, waitFor, 10*time.Millisecond)
}

func TestCacheReload(t *testing.T) {
	s := newSource(t, 1)
	c := newCache(t, s)

	s.set(3)
	value, err := c.Reload()
	require.NoError(t, err)
	require.Equal(t, 3, *value)
	require.Equal(t, 3, get(c))
}

func TestCacheReloadRetriesUntilSuccess(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	s.Lock()
	s.value = 4
	s.loadErrs = 3
	s.Unlock()
	value, err := c.Reload()
	require.NoError(t, err)
	require.Equal(t, 4, *value)
	require.Equal(t, 4, get(c))
}

func TestCacheInvalidate(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	s.Lock()
	s.value = 5
	s.loadErrs = 2
	s.Unlock()
	c.Invalidate()
	require.Nil(t, cached(c))
	require.Equal(t, 5, *c.Get())
}

func TestCacheSet(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)
	sub := c.Subscribe()

	c.Set(ptr(6))
	require.Equal(t, 6, get(c))
	select {
	case <-sub.Changed():
	case <-time.After(waitFor):
		t.Fatal("subscriber was not notified")
	}
	require.Equal(t, 6, *sub.Get())
}

func TestCacheInvalidateLoadsOnlyOnGet(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	// Invalidate only empties the cache: nothing loads until Get, and a Set
	// before that fills the cache without a load.
	loads := s.loadCount()
	c.Invalidate()
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, loads, s.loadCount())
	require.Nil(t, cached(c))

	c.Set(ptr(2))
	require.Equal(t, 2, *c.Get())
	require.Equal(t, loads, s.loadCount())
}

// A Set after a write waits for a load that is running: that load may have
// read the source before the write, and must not overwrite the Set.
// Reads keep the cached value while a reload backs off between failed
// attempts.
func TestCacheReadsDuringReloadBackoff(t *testing.T) {
	s := newSource(t, 1)
	c := newQuietCache(t, s)

	s.Lock()
	s.value = 2
	s.loadErrs = 1 << 20
	s.Unlock()
	reloaded := make(chan struct{})
	go func() {
		_, _ = c.Reload()
		close(reloaded)
	}()
	require.Eventually(t, func() bool { return s.loadCount() > 3 }, waitFor, 10*time.Millisecond)

	read := make(chan int, 1)
	go func() { read <- *c.Get() }()
	select {
	case value := <-read:
		require.Equal(t, 1, value)
	case <-time.After(waitFor):
		t.Fatal("Get waited for the failing reload")
	}

	s.Lock()
	s.loadErrs = 0
	s.Unlock()
	<-reloaded
	require.Equal(t, 2, get(c))
}

func TestCacheSetWaitsForRunningLoad(t *testing.T) {
	loading := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int32
	load := func(context.Context) (*int, error) {
		if loads.Add(1) == 2 {
			close(loading)
			<-release
		}
		return ptr(1), nil
	}
	c := New(t.Context(), load, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	_, err := c.Reload()
	require.NoError(t, err)

	reloaded := make(chan struct{})
	go func() {
		_, err := c.Reload()
		assert.NoError(t, err)
		close(reloaded)
	}()
	<-loading

	set := make(chan struct{})
	go func() {
		c.Set(ptr(2))
		close(set)
	}()
	select {
	case <-set:
		t.Fatal("Set ran during a load")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-reloaded
	<-set
	require.Equal(t, 2, get(c))
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

	require.Eventually(t, func() bool { return get(c) == 7 }, waitFor, 10*time.Millisecond)
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
	_, err := c.Reload()
	require.NoError(t, err)
	for _, sub := range []*Subscription[int]{first, second} {
		select {
		case <-sub.Changed():
		case <-time.After(waitFor):
			t.Fatal("subscriber was not notified")
		}
		require.Equal(t, 8, *sub.Get())
	}

	first.Close()
	first.Close()
	_, open := <-first.Changed()
	require.False(t, open)

	c.Set(ptr(9))
	select {
	case <-second.Changed():
	case <-time.After(waitFor):
		t.Fatal("remaining subscriber was not notified")
	}
	require.Equal(t, 9, *second.Get())
}

func TestCacheClose(t *testing.T) {
	s := newSource(t, 1)
	c := New(context.Background(), s.load, nil)
	_, err := c.Reload()
	require.NoError(t, err)

	sub := c.Subscribe()
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	_, open := <-sub.Changed()
	require.False(t, open)
	_, open = <-c.Subscribe().Changed()
	require.False(t, open)
	_, err = c.Reload()
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, get(c))
}
