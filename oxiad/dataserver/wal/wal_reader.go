// Copyright 2023-2025 The Oxia Authors
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

package wal

import (
	"sync"

	"github.com/oxia-db/oxia/proto"
)

func (t *wal) NewReader(after int64) (Reader, error) {
	firstOffset := after + 1

	if firstOffset < t.FirstOffset() {
		return nil, ErrEntryNotFound
	}

	r := &forwardReader{
		reader: reader{
			wal:        t,
			nextOffset: firstOffset,
			closed:     false,
		},
	}

	return r, nil
}

func (t *wal) NewReverseReader() (Reader, error) {
	r := &reverseReader{reader{
		wal:        t,
		nextOffset: t.LastOffset(),
		closed:     false,
	}}
	return r, nil
}

type reader struct {
	// wal the log to iterate
	wal *wal

	nextOffset int64

	closed bool
}

type forwardReader struct {
	reader
	sync.Mutex
}

type reverseReader struct {
	reader
}

func (r *forwardReader) Close() error {
	r.Lock()
	defer r.Unlock()
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	return nil
}

func (r *reverseReader) Close() error {
	r.closed = true
	return nil
}

func (r *forwardReader) ReadNext() (*proto.LogEntry, error) {
	timer := r.wal.readLatency.Timer()
	defer timer.Done()

	r.Lock()
	defer r.Unlock()

	if r.closed {
		return nil, ErrReaderClosed
	}

	index := r.nextOffset
	entry, err := r.wal.readAtIndex(index)
	if err != nil {
		return nil, err
	}

	r.nextOffset++
	return entry, nil
}

func (r *forwardReader) HasNext() bool {
	r.Lock()
	defer r.Unlock()

	if r.closed {
		return false
	}

	return r.nextOffset <= r.wal.LastOffset()
}

func (r *reverseReader) ReadNext() (*proto.LogEntry, error) {
	if r.closed {
		return nil, ErrReaderClosed
	}

	entry, err := r.wal.readAtIndex(r.nextOffset)
	if err != nil {
		return nil, err
	}
	// If we read the first entry, this overflows to MaxUint64
	r.nextOffset--
	return entry, nil
}

func (r *reverseReader) HasNext() bool {
	if r.closed {
		return false
	}

	firstOffset := r.wal.FirstOffset()
	return firstOffset != InvalidOffset && r.nextOffset != (firstOffset-1)
}
