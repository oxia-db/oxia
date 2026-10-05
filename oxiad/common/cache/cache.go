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
// changes only through Set.
type WatchFunc func(ctx context.Context) (<-chan struct{}, error)

// Cache holds the last value loaded from, or written to, a source. A nil
// value means the cache has none, and the next Get loads it.
type Cache[T any] struct {
	// Set once by New.
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	load   LoadFunc[T]
	watch  WatchFunc

	// mu guards the fields below it. Get takes the read lock. Set,
	// Invalidate and the loads take the write lock, and a load holds it until
	// it stores its value: a Set or Invalidate after a write to the source
	// then cannot be overwritten by a load that read the source before.
	mu          sync.RWMutex
	value       *T
	subscribers map[*Subscription[T]]struct{}
	closed      bool

	wg sync.WaitGroup
}

// New starts watching the source. The cache has no value until the first load
// or Set, which a watch session starting also triggers.
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
	if watch == nil {
		return c
	}
	c.wg.Go(func() { //nolint:contextcheck // runs on the cache context
		bo := oxiatime.NewBackOff(c.ctx)
		_ = backoff.RetryNotify(func() error {
			ctx, cancel := context.WithCancel(c.ctx)
			defer cancel()
			changes, err := c.watch(ctx)
			if err != nil {
				return err
			}
			// Changes made before the watch started were not observed by it.
			_, _ = c.Reload()
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
					_, _ = c.Reload()
				}
			}
		}, bo, func(err error, retryAfter time.Duration) {
			c.logger.Warn("Watch failed, restarting later",
				slog.Any("error", err), slog.Duration("retry-after", retryAfter))
		})
		c.logger.Info("Cache watcher closed")
	})
	return c
}

// Get returns the cached value. When the cache is empty, before the first
// load and after Invalidate, it loads the value, retrying until the cache has
// one. It never returns nil, except once the cache is closed while empty.
func (c *Cache[T]) Get() *T {
	c.mu.RLock()
	value := c.value
	c.mu.RUnlock()
	if value != nil {
		return value
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.value != nil {
		return c.value
	}
	value, _ = c.reload()
	return value
}

// Set stores a value the caller has written to the source, and notifies the
// subscribers. value must not be nil: use Invalidate to empty the cache.
func (c *Cache[T]) Set(value *T) {
	if value == nil {
		panic("cache: Set with a nil value, use Invalidate to empty the cache")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
	for s := range c.subscribers {
		channel.PushNoBlock(s.changed, struct{}{})
	}
}

// Invalidate empties the cache, so the next Get loads the value.
func (c *Cache[T]) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = nil
}

// Subscribe returns a subscription notified after each successful load or
// Set.
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

// Reload loads the value and stores it, retrying until a load succeeds. It
// only fails once the cache is closed.
func (c *Cache[T]) Reload() (*T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reload()
}

// reload is Reload, for callers holding mu.
func (c *Cache[T]) reload() (*T, error) {
	return backoff.RetryNotifyWithData(func() (*T, error) {
		select {
		case <-c.ctx.Done():
			return nil, backoff.Permanent(c.ctx.Err())
		default:
		}

		loadCtx, cancel := context.WithTimeout(c.ctx, loadTimeout)
		defer cancel()
		value, err := c.load(loadCtx)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, errors.New("load returned no value")
		}

		c.value = value
		for s := range c.subscribers {
			channel.PushNoBlock(s.changed, struct{}{})
		}
		return value, nil
	}, oxiatime.NewBackOff(c.ctx), func(err error, retryAfter time.Duration) {
		c.logger.Warn("Failed to load, retrying later",
			slog.Any("error", err), slog.Duration("retry-after", retryAfter))
	})
}

// Subscription is notified each time the cache gets a new value from a load
// or a Set.
type Subscription[T any] struct {
	cache   *Cache[T]
	changed chan struct{}
}

// Changed receives a value after each successful load or Set. Notifications
// are coalesced: a subscriber that falls behind receives one for several
// values, and reads the latest with Get. The channel is closed when the
// subscription or the cache is closed.
func (s *Subscription[T]) Changed() <-chan struct{} {
	return s.changed
}

// Get returns the cached value, as Cache.Get.
func (s *Subscription[T]) Get() *T {
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
