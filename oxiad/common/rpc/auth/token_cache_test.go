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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenCache(t *testing.T) {
	now := time.Now()
	token := "token"

	t.Run("empty", func(t *testing.T) {
		c := &tokenCache{}
		_, ok := c.get(token, now)
		assert.False(t, ok)
	})

	t.Run("expires with the token", func(t *testing.T) {
		c := &tokenCache{}
		c.put(token, "user", now.Add(10*time.Second), now)

		userName, ok := c.get(token, now.Add(10*time.Second-time.Nanosecond))
		assert.True(t, ok)
		assert.Equal(t, "user", userName)

		_, ok = c.get(token, now.Add(10*time.Second))
		assert.False(t, ok)
	})

	t.Run("expires after the max ttl", func(t *testing.T) {
		c := &tokenCache{}
		c.put(token, "user", now.Add(time.Hour), now)

		userName, ok := c.get(token, now.Add(tokenCacheMaxTTL-time.Nanosecond))
		assert.True(t, ok)
		assert.Equal(t, "user", userName)

		_, ok = c.get(token, now.Add(tokenCacheMaxTTL))
		assert.False(t, ok)
	})

	t.Run("bounded", func(t *testing.T) {
		c := &tokenCache{}
		for i := range tokenCacheMaxEntries {
			c.put(fmt.Sprintf("token-%d", i), "user", now.Add(time.Hour), now)
		}
		assert.Len(t, c.entries, tokenCacheMaxEntries)

		c.put(token, "user", now.Add(time.Hour), now)
		assert.Len(t, c.entries, 1)
		_, ok := c.get(token, now)
		assert.True(t, ok)
	})
}
