// Copyright 2025 StreamNative, Inc.
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

package kv

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/streamnative/oxia/common"
)

type SequenceWaiter interface {
	io.Closer

	Receive(context.Context) (string, error)

	Ch() <-chan string
}

type SequenceWaiterTracker interface {
	io.Closer
	AddSequenceWaiter(key string) *sequenceWaiter
	SequenceUpdated(prefixKey string, lastSequenceKey string)
}

type sequenceWaiterID int64

type sequenceWaiterTracker struct {
	sync.RWMutex
	waiters map[string]map[sequenceWaiterID]*sequenceWaiter
	idGen   atomic.Int64
}

type sequenceWaiter struct {
	key     string
	id      sequenceWaiterID
	och     common.OverrideChannel[string]
	tracker *sequenceWaiterTracker
}

func (sw *sequenceWaiter) Receive(ctx context.Context) (string, error) {
	return sw.och.Receive(ctx)
}

func (sw *sequenceWaiter) Ch() <-chan string {
	return sw.och.Ch()
}

func (sw *sequenceWaiter) Close() error {
	sw.tracker.remove(sw.key, sw.id)
	return nil
}

// ////////////////////////////////////////////////////////////////////////////

func NewSequencesWaitTracker() SequenceWaiterTracker {
	return &sequenceWaiterTracker{
		waiters: map[string]map[sequenceWaiterID]*sequenceWaiter{},
	}
}

func (swt *sequenceWaiterTracker) AddSequenceWaiter(key string) *sequenceWaiter {
	swt.Lock()
	defer swt.Unlock()

	im, existing := swt.waiters[key]
	if !existing {
		im = map[sequenceWaiterID]*sequenceWaiter{}
		swt.waiters[key] = im
	}

	id := sequenceWaiterID(swt.idGen.Add(1))
	sw := &sequenceWaiter{key, id, common.NewOverrideChannel[string](), swt}
	im[id] = sw
	return sw
}

func (swt *sequenceWaiterTracker) remove(key string, id sequenceWaiterID) {
	swt.Lock()
	defer swt.Unlock()

	im, existing := swt.waiters[key]
	if !existing {
		return
	}

	delete(im, id)
	if len(im) == 0 {
		delete(swt.waiters, key)
	}
}

func (swt *sequenceWaiterTracker) SequenceUpdated(prefixKey string, lastSequenceKey string) {
	swt.RLock()
	defer swt.RUnlock()

	for _, w := range swt.waiters[prefixKey] {
		w.och.WriteLast(lastSequenceKey)
	}
}

func (swt *sequenceWaiterTracker) Close() error {
	swt.Lock()
	defer swt.Unlock()

	for _, m := range swt.waiters {
		for _, w := range m {
			_ = w.Close()
		}

		clear(m)
	}

	clear(swt.waiters)
	return nil
}
