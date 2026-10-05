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

// Package cache keeps a single value loaded from a source, reloads it when the
// source reports a change, and lets callers subscribe to new values.
package cache

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/common/channel"
	oxiatime "github.com/oxia-db/oxia/common/time"
)

const loadTimeout = 30 * time.Second

// LoadFunc returns the current value of the source. ctx carries the load
// timeout. A failed load, or one that returns a nil value, is logged and
// retried with backoff until a load succeeds.
type LoadFunc[T any] func(ctx context.Context) (*T, error)

// WatchFunc starts watching the source and returns a channel that receives a
// value whenever the source may have changed. Signals may be coalesced: send
// without blocking into a channel with a buffer of 1. The cache stops reading
// the channel once ctx is done, when the watch session ends or the cache
// closes, and the watch should then stop. Closing the channel means the watch
// ended, and the cache starts a new one. A nil WatchFunc is for a source that
// changes only through Compute.
type WatchFunc func(ctx context.Context) (<-chan struct{}, error)

// Cache holds the last value loaded from, or written to, a source, like a
// loading cache. When it is empty, at first and after a failed write, the next
// Get or Compute loads the value, and reports when that load fails. A reload,
// after a change of the source, keeps the cached value until it succeeds.
type Cache[T any] struct {
	// Set once by New.
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	load   LoadFunc[T]
	watch  WatchFunc

	// value is read without a lock, and only changes under mu. It is nil when
	// the cache is empty.
	value atomic.Pointer[T]

	// mu serializes Compute and the loads: a load then cannot store a value
	// read before a Compute wrote the source. It also guards the fields below
	// it.
	mu          sync.Mutex
	subscribers map[*Subscription[T]]struct{}
	closed      bool

	wg sync.WaitGroup
}

// New starts watching the source. The cache is empty until the first Get,
// Compute or reload loads the value.
func New[T any](ctx context.Context, load LoadFunc[T], watch WatchFunc) *Cache[T] {
	cacheCtx, cancel := context.WithCancel(ctx)
	c := &Cache[T]{
		logger:      slog.With(slog.String("component", "cache")),
		ctx:         cacheCtx,
		cancel:      cancel,
		load:        load,
		watch:       watch,
		subscribers: map[*Subscription[T]]struct{}{},
	}
	if watch != nil {
		c.wg.Go(c.runWatcher)
	}
	return c
}

// Get returns the cached value. When the cache is empty, it loads the value,
// and returns the error of the load if it fails. Concurrent calls wait for
// the same load.
func (c *Cache[T]) Get() (*T, error) {
	if value := c.value.Load(); value != nil {
		return value, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if value := c.value.Load(); value != nil {
		return value, nil
	}
	return c.load0()
}

// Compute writes a new value to the source through fn, and stores the value
// fn returns. It loads the value first when the cache is empty. fn runs under
// the lock that serializes the loads, so no load can store a value read before
// the write. When fn fails, the write may still have been applied: the cache
// is emptied, so that the next Get or Compute loads the value.
func (c *Cache[T]) Compute(fn func(current *T) (*T, error)) (*T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.value.Load()
	if current == nil {
		var err error
		if current, err = c.load0(); err != nil {
			return nil, err
		}
	}
	next, err := fn(current)
	if err != nil {
		c.value.Store(nil)
		return nil, err
	}
	c.value.Store(next)
	for s := range c.subscribers {
		channel.PushNoBlock(s.changed, struct{}{})
	}
	return next, nil
}

// Reload loads the value and stores it, retrying until a load succeeds. It
// fails only if the cache is closed first, which keeps the cached value. Each
// attempt holds the lock while it loads and stores the value, and the backoff
// between attempts does not.
func (c *Cache[T]) Reload() error {
	return backoff.RetryNotify(func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, err := c.load0()
		return err
	}, oxiatime.NewBackOff(c.ctx), func(err error, retryAfter time.Duration) {
		c.logger.Warn("Failed to load, retrying later",
			slog.Any("error", err), slog.Duration("retry-after", retryAfter))
	})
}

// load0 loads the value and stores it, for callers holding mu.
func (c *Cache[T]) load0() (*T, error) {
	loadCtx, cancel := context.WithTimeout(c.ctx, loadTimeout)
	defer cancel()
	value, err := c.load(loadCtx)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("load returned no value")
	}
	c.value.Store(value)
	for s := range c.subscribers {
		channel.PushNoBlock(s.changed, struct{}{})
	}
	return value, nil
}

// Subscribe returns a subscription notified after each successful load or
// Compute.
func (c *Cache[T]) Subscribe() *Subscription[T] {
	s := &Subscription[T]{cache: c, changed: make(chan struct{}, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		close(s.changed)
		return s
	}
	c.subscribers[s] = struct{}{}
	return s
}

// Close cancels the running load and watch, and closes all subscriptions.
func (c *Cache[T]) Close() error {
	c.cancel()
	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	for s := range c.subscribers {
		close(s.changed)
	}
	clear(c.subscribers)
	return nil
}

// runWatcher runs the watch until the cache closes, reloading the value on
// each change, and starts a new watch when one fails.
func (c *Cache[T]) runWatcher() {
	bo := oxiatime.NewBackOff(c.ctx)
	_ = backoff.RetryNotify(func() error {
		ctx, cancel := context.WithCancel(c.ctx)
		defer cancel()
		changes, err := c.watch(ctx)
		if err != nil {
			return err
		}
		// Changes made before the watch started were not observed by it.
		_ = c.Reload()
		for {
			select {
			case <-ctx.Done():
				return nil
			case _, ok := <-changes:
				if !ok {
					return errors.New("watch ended")
				}
				// The watch is healthy: a restart starts the backoff over.
				bo.Reset()
				_ = c.Reload()
			}
		}
	}, bo, func(err error, retryAfter time.Duration) {
		c.logger.Warn("Watch failed, restarting later",
			slog.Any("error", err), slog.Duration("retry-after", retryAfter))
	})
	c.logger.Info("Cache watcher closed")
}

// Subscription is notified each time the cache gets a new value from a load
// or a Compute.
type Subscription[T any] struct {
	cache   *Cache[T]
	changed chan struct{}
}

// Changed receives a value after each successful load or Compute.
// Notifications are coalesced: a subscriber that falls behind receives one
// for several values, and reads the latest with Get. The channel is closed when the
// subscription or the cache is closed.
func (s *Subscription[T]) Changed() <-chan struct{} {
	return s.changed
}

// Get returns the cached value, as Cache.Get.
func (s *Subscription[T]) Get() (*T, error) {
	return s.cache.Get()
}

// Close stops the notifications and closes the Changed channel.
func (s *Subscription[T]) Close() {
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	if _, exists := s.cache.subscribers[s]; !exists {
		return
	}
	delete(s.cache.subscribers, s)
	close(s.changed)
}
