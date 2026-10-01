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

package lead

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// BenchmarkQuorumAckTracker measures the leader-side cost of tracking the
// quorum for each wal entry with RF=3: the head advances by a batch of
// entries, then both followers confirm the whole batch with one cumulative
// ack. Two goroutines stay parked on the tracker condition, like the follower
// cursors waiting at the head of the wal, so every broadcast has waiters to
// wake. Results are per wal entry.
func BenchmarkQuorumAckTracker(b *testing.B) {
	for _, batch := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			benchmarkQuorumAckTracker(b, batch)
		})
	}
}

func benchmarkQuorumAckTracker(b *testing.B, batch int) {
	b.Helper()
	at := NewQuorumAckTracker(3, wal.InvalidOffset, wal.InvalidOffset)
	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	if err != nil {
		b.Fatal(err)
	}
	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	for range 2 {
		wg.Go(func() {
			for ctx.Err() == nil {
				_ = at.WaitForHeadOffsetOrCommitAdvance(ctx, math.MaxInt64, at.CommitOffset())
			}
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	head := wal.InvalidOffset
	for i := 0; i < b.N; i += batch {
		for range min(batch, b.N-i) {
			head++
			at.AdvanceHeadOffset(head)
		}
		c1.Ack(head)
		c2.Ack(head)
	}
	b.StopTimer()

	cancel()
	_ = at.Close()
	wg.Wait()
}
