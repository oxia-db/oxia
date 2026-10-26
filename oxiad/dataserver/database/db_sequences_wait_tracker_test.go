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
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequenceWaiterTrackerAddSequenceWaiterReturnsTheLastKey(t *testing.T) {
	swt := NewSequencesWaitTracker()

	w, err := swt.AddSequenceWaiter("key", func() (string, bool, error) { return "key-1", true, nil })
	require.NoError(t, err)
	defer w.Close()

	assert.Equal(t, "key-1", nextKey(t, w))
	assertNoNextKey(t, w)
}

// A commit that the read of the last key misses must not be overridden by the
// key that the read returns.
func TestSequenceWaiterTrackerAddSequenceWaiterKeepsNewerKey(t *testing.T) {
	swt := NewSequencesWaitTracker()

	w, err := swt.AddSequenceWaiter("key", func() (string, bool, error) {
		// The commit of key-2 completes while the read returns key-1
		swt.SequenceUpdated("key", "key-2")
		return "key-1", true, nil
	})
	require.NoError(t, err)
	defer w.Close()

	assert.Equal(t, "key-2", nextKey(t, w))
	assertNoNextKey(t, w)
}

func TestSequenceWaiterTrackerAddSequenceWaiterFailsWhenTheReadFails(t *testing.T) {
	swt := NewSequencesWaitTracker().(*sequenceWaiterTracker)
	readErr := errors.New("read failure")

	w, err := swt.AddSequenceWaiter("key", func() (string, bool, error) { return "", false, readErr })
	assert.ErrorIs(t, err, readErr)
	assert.Nil(t, w)
	assert.Empty(t, swt.sequences)
}

func TestSequenceWaiterTrackerSequenceUpdated(t *testing.T) {
	swt := NewSequencesWaitTracker()
	w1 := addSequenceWaiter(t, swt, "key-1")
	w2 := addSequenceWaiter(t, swt, "key-2")
	w3 := addSequenceWaiter(t, swt, "key-1")

	swt.SequenceUpdated("non-existing", "non-existing-123")
	swt.SequenceUpdated("key-1", "key-1-123")
	assert.Equal(t, "key-1-123", nextKey(t, w1))
	assert.Equal(t, "key-1-123", nextKey(t, w3))
	assertNoNextKey(t, w2)

	// An older or the same key is not a new last key
	swt.SequenceUpdated("key-1", "key-1-100")
	swt.SequenceUpdated("key-1", "key-1-123")
	assertNoNextKey(t, w1)

	// Only the last of the keys updated meanwhile is returned
	swt.SequenceUpdated("key-1", "key-1-456")
	swt.SequenceUpdated("key-1", "key-1-789")
	assert.Equal(t, "key-1-789", nextKey(t, w1))

	w1.Close()
	w2.Close()
	w3.Close()
}

func TestSequenceWaiterNextWaitsForANewKey(t *testing.T) {
	swt := NewSequencesWaitTracker()
	w := addSequenceWaiter(t, swt, "key")
	defer w.Close()

	next := make(chan string)
	go func() {
		key, err := w.Next(context.Background())
		assert.NoError(t, err)
		next <- key
	}()
	swt.SequenceUpdated("key", "key-1")
	assert.Equal(t, "key-1", <-next)
}

func TestSequenceWaiterNextReturnsWhenTheContextIsDone(t *testing.T) {
	swt := NewSequencesWaitTracker()
	w := addSequenceWaiter(t, swt, "key")
	defer w.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := w.Next(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSequenceWaiterCloseRemovesTheSequenceWithItsLastWaiter(t *testing.T) {
	swt := NewSequencesWaitTracker().(*sequenceWaiterTracker)
	w1 := addSequenceWaiter(t, swt, "key")
	w2 := addSequenceWaiter(t, swt, "key")

	w1.Close()
	w1.Close()
	assert.Contains(t, swt.sequences, "key")

	w2.Close()
	assert.Empty(t, swt.sequences)

	// A key of a sequence without waiters is not kept
	swt.SequenceUpdated("key", "key-1")
	w3 := addSequenceWaiter(t, swt, "key")
	defer w3.Close()
	assertNoNextKey(t, w3)
}

func TestSequenceWaiterTrackerCloseEndsTheWaiters(t *testing.T) {
	swt := NewSequencesWaitTracker()
	w := addSequenceWaiter(t, swt, "key")
	swt.SequenceUpdated("key", "key-1")

	swt.Close()
	_, err := w.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	w.Close()

	// A waiter added once the tracker is closed is closed too
	w = addSequenceWaiter(t, swt, "key")
	_, err = w.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	w.Close()
}

func addSequenceWaiter(t *testing.T, swt SequenceWaiterTracker, key string) SequenceWaiter {
	t.Helper()
	w, err := swt.AddSequenceWaiter(key, func() (string, bool, error) { return "", false, nil })
	require.NoError(t, err)
	return w
}

func nextKey(t *testing.T, w SequenceWaiter) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key, err := w.Next(ctx)
	require.NoError(t, err)
	return key
}

func assertNoNextKey(t *testing.T, w SequenceWaiter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := w.Next(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// dropNextKey drops the last key of the sequence, if there is one.
func dropNextKey(t *testing.T, w SequenceWaiter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := w.Next(ctx); err != nil {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
}
