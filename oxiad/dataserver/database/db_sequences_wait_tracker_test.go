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

package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequencesWaitTracker(t *testing.T) {
	swt := NewSequencesWaitTracker()

	w1 := addSequenceWaiter(t, swt, "key-1")
	w2 := addSequenceWaiter(t, swt, "key-2")

	assert.Empty(t, w1.Ch())
	assert.Empty(t, w2.Ch())

	swt.SequenceUpdated("non-existing", "non-existing-123")
	assert.Empty(t, w1.Ch())
	assert.Empty(t, w2.Ch())

	swt.SequenceUpdated("key-1", "key-1-123")
	x1, err := w1.Receive(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "key-1-123", x1)
	assert.Empty(t, w2.Ch())

	swt.SequenceUpdated("key-2", "key-2-456")
	assert.Empty(t, w1.Ch())
	x2, err := w2.Receive(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "key-2-456", x2)

	w3 := addSequenceWaiter(t, swt, "key-1")

	swt.SequenceUpdated("key-1", "key-1-890")
	x3 := <-w1.Ch()
	assert.Equal(t, "key-1-890", x3)
	x4 := <-w3.Ch()
	assert.Equal(t, "key-1-890", x4)
	assert.Empty(t, w2.Ch())

	assert.NoError(t, w1.Close())

	swt.SequenceUpdated("key-1", "key-1-234")
	assert.Empty(t, w1.Ch())
	x5 := <-w3.Ch()
	assert.Equal(t, "key-1-234", x5)
	assert.Empty(t, w2.Ch())

	assert.Empty(t, w3.Ch())

	ch3 := w3.Ch()
	_ = w3.Close()
	swt.SequenceUpdated("key-1", "key-1-234")
	assert.Empty(t, ch3)
}

func TestSequencesWaitTrackerMultipleClose(t *testing.T) {
	swt := NewSequencesWaitTracker()

	w1 := addSequenceWaiter(t, swt, "key-1")
	assert.Empty(t, swt.Close())

	assert.NoError(t, w1.Close())
}

// A commit that the read of the last key misses must not be overridden by the
// key that the read returns.
func TestSequencesWaitTrackerAddSequenceWaiterKeepsNewerKey(t *testing.T) {
	swt := NewSequencesWaitTracker()

	updated := make(chan struct{})
	w, err := swt.AddSequenceWaiter("key", func() (string, bool, error) {
		// The commit of key-2 completes while the read returns key-1
		go func() {
			swt.SequenceUpdated("key", "key-2")
			close(updated)
		}()
		select {
		case <-updated:
		case <-time.After(100 * time.Millisecond):
		}
		return "key-1", true, nil
	})
	require.NoError(t, err)
	<-updated

	last, err := w.Receive(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "key-2", last)
	assert.NoError(t, w.Close())
}

func addSequenceWaiter(t *testing.T, swt SequenceWaiterTracker, key string) *sequenceWaiter {
	t.Helper()
	w, err := swt.AddSequenceWaiter(key, func() (string, bool, error) { return "", false, nil })
	require.NoError(t, err)
	return w
}
