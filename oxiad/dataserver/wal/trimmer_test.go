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

package wal

import (
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/logging"

	"github.com/oxia-db/oxia/common/proto"
)

func init() {
	logging.ConfigureLogger()
}

type mockedCommitOffsetProvider struct {
	commitOffset atomic.Int64
}

func (p *mockedCommitOffsetProvider) CommitOffset() int64 {
	return p.commitOffset.Load()
}

// hookedCommitOffsetProvider runs a hook from the next CommitOffset call: the
// trimming makes it once it computed the trim offset on the wal entries.
type hookedCommitOffsetProvider struct {
	mockedCommitOffsetProvider
	hook atomic.Pointer[func()]
}

func (p *hookedCommitOffsetProvider) CommitOffset() int64 {
	if hook := p.hook.Swap(nil); hook != nil {
		(*hook)()
	}
	return p.mockedCommitOffsetProvider.CommitOffset()
}

// hookedSegmentsGroup runs a hook when the trimming deletes its segments.
type hookedSegmentsGroup struct {
	ReadOnlySegmentsGroup
	hook func()
}

func (g *hookedSegmentsGroup) TrimSegments(offset int64) error {
	g.hook()
	return g.ReadOnlySegmentsGroup.TrimSegments(offset)
}

// A follower clears its wal when it installs a snapshot, and truncates it for a
// new leader, while the trimming runs without holding the wal lock. The trim
// offset computed on the entries they drop must not apply to the entries that
// the wal gets after them.
func TestWalTrimmerDroppedEntries(t *testing.T) {
	clearWal := func(w Wal) error { return w.Clear() }
	truncateWal := func(lastSafeOffset int64) func(w Wal) error {
		return func(w Wal) error {
			_, err := w.TruncateLog(lastSafeOffset)
			return err
		}
	}
	for _, test := range []struct {
		name string
		drop func(w Wal) error
		// Drop the entries while the trimming deletes the segments, instead
		// of before it starts trimming the wal
		whileDeletingSegments bool
		// The offset of the entry the wal gets after the drop
		nextOffset int64
		// The first offset of the wal after it gets that entry
		firstOffset int64
	}{
		{name: "clear", drop: clearWal, nextOffset: 200, firstOffset: 200},
		{name: "clear-while-deleting-segments", drop: clearWal, whileDeletingSegments: true,
			nextOffset: 200, firstOffset: 200},
		{name: "truncate-all", drop: truncateWal(InvalidOffset), nextOffset: 200, firstOffset: 200},
		{name: "truncate", drop: truncateWal(98), nextOffset: 99, firstOffset: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := &FactoryOptions{
				BaseWalDir:  t.TempDir(),
				Retention:   2 * time.Millisecond,
				SegmentSize: 1024,
			}
			clock := &time2.MockedClock{}
			commitOffsetProvider := &hookedCommitOffsetProvider{}
			commitOffsetProvider.commitOffset.Store(math.MaxInt64)

			// The trimming only runs when the test calls it
			w, err := newWal(constant.DefaultNamespace, 1, options, commitOffsetProvider, clock, time.Hour)
			require.NoError(t, err)
			impl := w.(*wal)
			trimmer := impl.trimmer.(*trimmer)
			appendEntries := func(firstOffset, lastOffset int64) {
				for i := firstOffset; i <= lastOffset; i++ {
					require.NoError(t, w.Append(&proto.LogEntry{
						Offset:    i,
						Value:     []byte(fmt.Sprintf("%d", i)),
						Timestamp: uint64(i),
					}))
				}
			}
			appendEntries(0, 99)

			drop := func() { require.NoError(t, test.drop(w)) }
			if test.whileDeletingSegments {
				impl.Lock()
				impl.readOnlySegments = &hookedSegmentsGroup{ReadOnlySegmentsGroup: impl.readOnlySegments, hook: drop}
				impl.Unlock()
			} else {
				commitOffsetProvider.hook.Store(&drop)
			}

			// The trimming computes the trim offset 99, on the entries before
			// the drop
			clock.Set(101)
			require.NoError(t, trimmer.doTrim())

			appendEntries(test.nextOffset, test.nextOffset+99)
			assert.EqualValues(t, test.firstOffset, w.FirstOffset())

			// The wal holds the entries from its first offset
			_, err = w.NewReader(test.firstOffset - 2)
			assert.ErrorIs(t, err, ErrEntryNotFound)
			r, err := w.NewReader(test.firstOffset - 1)
			require.NoError(t, err)
			for offset := test.firstOffset; offset <= w.LastOffset(); offset++ {
				entry, _, _, err := r.ReadNext()
				require.NoError(t, err)
				assert.EqualValues(t, offset, entry.Offset)
			}
			assert.NoError(t, r.Close())

			// And the next trimming trims them
			clock.Set(test.nextOffset + 52)
			require.NoError(t, trimmer.doTrim())
			assert.EqualValues(t, test.nextOffset+50, w.FirstOffset())

			assert.NoError(t, w.Close())
		})
	}
}

func TestWalTrimmer(t *testing.T) {
	options := &FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   2 * time.Millisecond,
		SegmentSize: 10 * 1024,
	}

	clock := &time2.MockedClock{}
	commitOffsetProvider := &mockedCommitOffsetProvider{}
	commitOffsetProvider.commitOffset.Store(math.MaxInt64)

	w, err := newWal(constant.DefaultNamespace, 1, options, commitOffsetProvider, clock, 10*time.Millisecond)
	assert.NoError(t, err)

	for i := int64(0); i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:      0,
			Offset:    i,
			Value:     []byte(fmt.Sprintf("%d", i)),
			Timestamp: uint64(i),
		}))
	}

	clock.Set(2)

	// Should not get triggered since there are not expired entries yet
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	clock.Set(5)

	assert.Eventually(t, func() bool {
		slog.Info(
			"checking...",
			slog.Int64("first-offset", w.FirstOffset()),
		)
		return w.FirstOffset() == 3
	}, 10*time.Second, 10*time.Millisecond)

	clock.Set(89)

	assert.Eventually(t, func() bool {
		slog.Info(
			"checking...",
			slog.Int64("first-offset", w.FirstOffset()),
		)
		return w.FirstOffset() == 87
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, w.Close())
}

func TestWalTrimUpToCommitOffset(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			options := &FactoryOptions{
				BaseWalDir:  t.TempDir(),
				Retention:   2 * time.Millisecond,
				SegmentSize: 128 * 1024,
			}
			slog.Info("Starting",
				slog.String("TestName", t.Name()),
				slog.String("BaseWalDir", t.TempDir()),
			)
			clock := &time2.MockedClock{}
			commitOffsetProvider := &mockedCommitOffsetProvider{}
			commitOffsetProvider.commitOffset.Store(math.MaxInt64)

			w, err := newWal(constant.DefaultNamespace, 1, options, commitOffsetProvider, clock, 10*time.Millisecond)
			assert.NoError(t, err)

			commitOffsetProvider.commitOffset.Store(-1)

			for i := int64(0); i < 100; i++ {
				assert.NoError(t, w.Append(&proto.LogEntry{
					Term:      0,
					Offset:    i,
					Value:     []byte(fmt.Sprintf("%d", i)),
					Timestamp: uint64(i),
				}))
			}

			clock.Set(5)
			time.Sleep(100 * time.Microsecond)

			// No trimming should happen yet, because of commit offset
			assert.EqualValues(t, 0, w.FirstOffset())
			assert.EqualValues(t, 99, w.LastOffset())

			commitOffsetProvider.commitOffset.Store(2)

			assert.Eventually(t, func() bool {
				offset := w.FirstOffset()
				slog.Info(
					"checking...",
					slog.Int64("first-offset", offset),
					slog.String("TestName", t.Name()),
				)
				return offset == 2
			}, 10*time.Second, 10*time.Millisecond)

			clock.Set(89)
			time.Sleep(100 * time.Microsecond)

			// No trimming should happen yet, because of commit offset
			assert.EqualValues(t, 2, w.FirstOffset())
			assert.EqualValues(t, 99, w.LastOffset())

			commitOffsetProvider.commitOffset.Store(100)

			assert.Eventually(t, func() bool {
				offset := w.FirstOffset()
				slog.Info(
					"checking...",
					slog.Int64("first-offset", offset),
					slog.String("TestName", t.Name()),
				)
				return offset == 87
			}, 10*time.Second, 10*time.Millisecond)

			slog.Info("Starting to close wal")
			assert.NoError(t, w.Close())
		})
	}
}
