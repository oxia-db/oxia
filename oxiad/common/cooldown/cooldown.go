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

// Package cooldown tracks the keys that failed, and when each may be tried
// again: the wait doubles after each failure, up to a limit.
package cooldown

import (
	"time"
)

// Cooldown is not safe for concurrent use: its owner calls it from one
// goroutine.
type Cooldown[K comparable] struct {
	entries map[K]*entry
}

type entry struct {
	wait  time.Duration
	until time.Time
}

func New[K comparable]() *Cooldown[K] {
	return &Cooldown[K]{entries: make(map[K]*entry)}
}

// Fail makes key wait initial after its first failure, and twice its last wait
// after the next ones, up to limit.
func (c *Cooldown[K]) Fail(key K, initial time.Duration, limit time.Duration) {
	e, ok := c.entries[key]
	if !ok {
		e = &entry{wait: initial}
		c.entries[key] = e
	} else {
		e.wait = min(2*e.wait, limit)
	}
	e.until = time.Now().Add(e.wait)
}

// Succeed forgets the failures of key.
func (c *Cooldown[K]) Succeed(key K) {
	delete(c.entries, key)
}

// Waiting reports whether key is still waiting after a failure.
func (c *Cooldown[K]) Waiting(key K) bool {
	e, ok := c.entries[key]
	return ok && time.Now().Before(e.until)
}

// Prune forgets the keys whose wait ended at least age ago: their next failure
// waits the initial time again.
func (c *Cooldown[K]) Prune(age time.Duration) {
	now := time.Now()
	for key, e := range c.entries {
		if now.Sub(e.until) >= age {
			delete(c.entries, key)
		}
	}
}
