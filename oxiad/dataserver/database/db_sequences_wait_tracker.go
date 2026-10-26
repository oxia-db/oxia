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
	"io"
	"sync"

	"github.com/oxia-db/oxia/oxiad/common/watch"
)

type SequenceWaiter interface {
	// Next waits until the last key of the sequence is newer than the one it
	// returned before, and returns it. It returns io.EOF once the tracker is
	// closed.
	Next(ctx context.Context) (string, error)

	Close()
}

type SequenceWaiterTracker interface {
	// AddSequenceWaiter registers a waiter for the sequence, and gives it the
	// last key that loadFunc returns, unless a newer one was already updated.
	// The waiter is registered before the read, so a commit that the read misses
	// still updates it.
	AddSequenceWaiter(key string, loadFunc func() (string, bool, error)) (SequenceWaiter, error)
	SequenceUpdated(prefixKey string, lastSequenceKey string)
	Close()
}

// lastSequenceKey is the last key of a sequence that the waiters see, or
// closed once the tracker is closed.
type lastSequenceKey struct {
	key    string
	closed bool
}

type sequenceWatch struct {
	watch   *watch.Watch[lastSequenceKey]
	waiters int
}

type sequenceWaiterTracker struct {
	sync.Mutex
	// sequences holds the last key of each sequence that has waiters. Owner:
	// the tracker, which creates a sequence's watch with its first waiter,
	// publishes newer keys, and removes it with its last waiter. Borrowers: the
	// waiters, which only receive from the watch. Guarded by the tracker's lock.
	sequences map[string]*sequenceWatch
	closed    bool
}

func NewSequencesWaitTracker() SequenceWaiterTracker {
	return &sequenceWaiterTracker{
		sequences: map[string]*sequenceWatch{},
	}
}

func (swt *sequenceWaiterTracker) AddSequenceWaiter(key string,
	loadFunc func() (string, bool, error)) (SequenceWaiter, error) {
	swt.Lock()
	sequence, ok := swt.sequences[key]
	if !ok {
		sequence = &sequenceWatch{watch: watch.New(lastSequenceKey{closed: swt.closed})}
		if !swt.closed {
			swt.sequences[key] = sequence
		}
	}
	sequence.waiters++
	sw := &sequenceWaiter{tracker: swt, key: key, receiver: sequence.watch.Subscribe()}
	swt.Unlock()

	lastKey, found, err := loadFunc()
	if err != nil {
		sw.Close()
		return nil, err
	}
	if found {
		swt.SequenceUpdated(key, lastKey)
	}
	return sw, nil
}

// SequenceUpdated publishes the last key of a sequence to its waiters, unless
// they already have a newer one: the keys of a sequence are zero-padded, so
// they order as strings.
func (swt *sequenceWaiterTracker) SequenceUpdated(prefixKey string, lastKey string) {
	swt.Lock()
	defer swt.Unlock()

	if sequence, ok := swt.sequences[prefixKey]; ok && lastKey > sequence.watch.Load().key {
		sequence.watch.Publish(lastSequenceKey{key: lastKey})
	}
}

func (swt *sequenceWaiterTracker) remove(key string) {
	swt.Lock()
	defer swt.Unlock()

	if sequence, ok := swt.sequences[key]; ok {
		sequence.waiters--
		if sequence.waiters == 0 {
			delete(swt.sequences, key)
		}
	}
}

func (swt *sequenceWaiterTracker) Close() {
	swt.Lock()
	defer swt.Unlock()

	swt.closed = true
	for _, sequence := range swt.sequences {
		sequence.watch.Publish(lastSequenceKey{key: sequence.watch.Load().key, closed: true})
	}
	clear(swt.sequences)
}

type sequenceWaiter struct {
	tracker  *sequenceWaiterTracker
	key      string
	receiver *watch.Receiver[lastSequenceKey]
	lastKey  string
	once     sync.Once
}

func (sw *sequenceWaiter) Next(ctx context.Context) (string, error) {
	for {
		last := sw.receiver.Load()
		if last.closed {
			return "", io.EOF
		}
		if last.key != sw.lastKey {
			sw.lastKey = last.key
			return last.key, nil
		}
		select {
		case <-sw.receiver.Changed():
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (sw *sequenceWaiter) Close() {
	sw.once.Do(func() { sw.tracker.remove(sw.key) })
}
