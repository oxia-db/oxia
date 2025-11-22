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
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/logging"
	time2 "github.com/oxia-db/oxia/common/time"

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
