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

package auth

import (
	"sync"
	"time"
)

const (
	// tokenCacheMaxTTL bounds how long a verified token is accepted without
	// being verified again, even when it expires later.
	tokenCacheMaxTTL = time.Minute

	// tokenCacheMaxEntries bounds the memory of the cache.
	tokenCacheMaxEntries = 10_000
)

type tokenCacheEntry struct {
	userName string
	expiry   time.Time
}

// tokenCache remembers the tokens that passed the verification, so that the
// RPCs carrying the same token skip the signature check. It is keyed by the
// token itself: a digest would cost more than the rest of the lookup, and the
// request metadata already keeps the token in memory.
type tokenCache struct {
	mu      sync.RWMutex
	entries map[string]tokenCacheEntry
}

// get returns the user name of a cached token, unless its entry expired.
func (c *tokenCache) get(token string, now time.Time) (string, bool) {
	c.mu.RLock()
	entry, ok := c.entries[token]
	c.mu.RUnlock()
	if !ok || !now.Before(entry.expiry) {
		return "", false
	}
	return entry.userName, true
}

// put caches a verified token until its expiry, and for tokenCacheMaxTTL at
// most. A full cache is emptied first.
func (c *tokenCache) put(token, userName string, expiry, now time.Time) {
	if maxExpiry := now.Add(tokenCacheMaxTTL); expiry.After(maxExpiry) {
		expiry = maxExpiry
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) >= tokenCacheMaxEntries {
		c.entries = make(map[string]tokenCacheEntry)
	}
	c.entries[token] = tokenCacheEntry{userName: userName, expiry: expiry}
}
