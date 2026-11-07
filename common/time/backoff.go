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

package time

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
)

const (
	// DefaultInitialInterval is the first interval of NewBackOff.
	DefaultInitialInterval = 100 * time.Millisecond
	// SlowInitialInterval is a first interval for retrying a remote server
	// that is likely down, e.g. a data server that failed its health checks.
	SlowInitialInterval = 10 * time.Second
)

func NewBackOff(ctx context.Context) backoff.BackOff {
	return NewBackOffWithInitialInterval(ctx, DefaultInitialInterval)
}

// NewBackOffWithMaxInterval is NewBackOff, with a cap on the interval. The default one grows to a minute, so a caller
// that has to notice a recovery within seconds would find its next attempt scheduled after the recovery.
func NewBackOffWithMaxInterval(ctx context.Context, maxInterval time.Duration) backoff.BackOff {
	return newBackOff(ctx, DefaultInitialInterval, maxInterval)
}

func NewBackOffWithInitialInterval(ctx context.Context, initialInterval time.Duration) backoff.BackOff {
	return newBackOff(ctx, initialInterval, backoff.DefaultMaxInterval)
}

func newBackOff(ctx context.Context, initialInterval time.Duration, maxInterval time.Duration) backoff.BackOff {
	return backoff.WithContext(&backoff.ExponentialBackOff{
		InitialInterval:     initialInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         maxInterval,
		MaxElapsedTime:      0, // Never stop trying
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}, ctx)
}

// ConcurrentBackOff wraps a backoff.BackOff with a mutex to make it safe
// for concurrent use across multiple goroutines.
type ConcurrentBackOff struct {
	mu sync.Mutex
	bo backoff.BackOff
}

var _ backoff.BackOffContext = (*ConcurrentBackOff)(nil)

func NewConcurrentBackOff(bo backoff.BackOff) *ConcurrentBackOff {
	return &ConcurrentBackOff{bo: bo}
}

// Context exposes the context of the wrapped backoff. backoff.RetryNotify
// only interrupts its wait between retries through a backoff.BackOffContext,
// so without it a cancelled context would not stop the wait.
func (c *ConcurrentBackOff) Context() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bc, ok := c.bo.(backoff.BackOffContext); ok {
		return bc.Context()
	}
	return context.Background()
}

func (c *ConcurrentBackOff) NextBackOff() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bo.NextBackOff()
}

func (c *ConcurrentBackOff) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bo.Reset()
}
