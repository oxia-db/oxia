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
	"errors"
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

func (*mockedCommitOffsetProvider) FlushDatabase() error {
	return nil
}

type flushingCommitOffsetProvider struct {
	mockedCommitOffsetProvider
	flush func() error
}

func (p *flushingCommitOffsetProvider) FlushDatabase() error {
	return p.flush()
}

// The database can hold the entries of the segments to delete in memory only:
// the trimming flushes it first, and deletes no segment when the flush fails.
func TestWalTrimmerFlushesDatabaseBeforeDeletingSegments(t *testing.T) {
	options := &FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   2 * time.Millisecond,
		SegmentSize: 1024,
	}
	segmentsPath := walPath(options.BaseWalDir, constant.DefaultNamespace, 1)

	clock := &time2.MockedClock{}
	commitOffsetProvider := &flushingCommitOffsetProvider{}
	commitOffsetProvider.commitOffset.Store(math.MaxInt64)
	flushes := atomic.Int64{}
	failFlush := atomic.Bool{}
	commitOffsetProvider.flush = func() error {
		flushes.Add(1)
		segments, err := listAllSegments(segmentsPath)
		assert.NoError(t, err)
		assert.Contains(t, segments, int64(0))
		if failFlush.Load() {
			return errors.New("failed to flush")
		}
		return nil
	}

	w, err := newWal(constant.DefaultNamespace, 1, options, commitOffsetProvider, clock, 10*time.Millisecond)
	require.NoError(t, err)

	for i := int64(0); i < 100; i++ {
		require.NoError(t, w.Append(&proto.LogEntry{
			Term:      0,
			Offset:    i,
			Value:     []byte(fmt.Sprintf("%d", i)),
			Timestamp: uint64(i),
		}))
	}
	segments, err := listAllSegments(segmentsPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 3)
	secondSegment := segments[1]

	// Trimming the first segment partially deletes none, and doesn't flush
	clock.Set(secondSegment + 1)
	assert.Eventually(t, func() bool {
		return w.FirstOffset() == secondSegment-1
	}, 10*time.Second, 10*time.Millisecond)
	assert.Zero(t, flushes.Load())

	// Trimming past it deletes it once the flush succeeds
	failFlush.Store(true)
	clock.Set(secondSegment + 2)
	assert.Eventually(t, func() bool {
		return flushes.Load() > 1
	}, 10*time.Second, 10*time.Millisecond)
	assert.EqualValues(t, secondSegment-1, w.FirstOffset())
	segments, err = listAllSegments(segmentsPath)
	require.NoError(t, err)
	assert.Contains(t, segments, int64(0))

	failFlush.Store(false)
	assert.Eventually(t, func() bool {
		return w.FirstOffset() == secondSegment
	}, 10*time.Second, 10*time.Millisecond)
	segments, err = listAllSegments(segmentsPath)
	require.NoError(t, err)
	assert.NotContains(t, segments, int64(0))

	assert.NoError(t, w.Close())
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
