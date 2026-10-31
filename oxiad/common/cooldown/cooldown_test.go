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

package cooldown

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCooldownFailDoublesTheWaitUpToTheLimit(t *testing.T) {
	c := New[string]()

	c.Fail("a", time.Hour, 3*time.Hour)
	assert.Equal(t, time.Hour, c.entries["a"].wait)
	c.Fail("a", time.Hour, 3*time.Hour)
	assert.Equal(t, 2*time.Hour, c.entries["a"].wait)
	c.Fail("a", time.Hour, 3*time.Hour)
	assert.Equal(t, 3*time.Hour, c.entries["a"].wait)
	assert.True(t, c.Waiting("a"))
	assert.False(t, c.Waiting("b"))
}

func TestCooldownSucceedForgetsTheFailures(t *testing.T) {
	c := New[string]()
	c.Fail("a", time.Hour, time.Hour)

	c.Succeed("a")

	assert.False(t, c.Waiting("a"))
	assert.Empty(t, c.entries)
}

func TestCooldownWaitingEndsAfterTheWait(t *testing.T) {
	c := New[string]()

	c.Fail("a", time.Nanosecond, time.Nanosecond)

	assert.Eventually(t, func() bool { return !c.Waiting("a") }, time.Second, time.Millisecond)
}

func TestCooldownPruneForgetsOldFailures(t *testing.T) {
	c := New[string]()
	c.Fail("old", time.Nanosecond, time.Nanosecond)
	c.Fail("recent", time.Hour, time.Hour)

	c.Prune(0)

	assert.NotContains(t, c.entries, "old")
	assert.Contains(t, c.entries, "recent")
}
