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

//go:build !disable_trap

package lead

import (
	"context"
	"sync/atomic"
	"time"
)

// observerCommitAdvertisementDelay is a test hook: an observer cursor parked at
// the head of the wal holds back the advertisement of a new commit offset for
// up to this long, unless a new entry arrives in the meantime to carry it (see
// followerCursor.waitAtHead). It widens the window in which a split child has
// received the tail of the parent's wal, but not the commit offset that covers
// it.
var observerCommitAdvertisementDelay atomic.Int64

// SetObserverCommitAdvertisementDelay sets the delay of the commit offset
// advertisements of the observer cursors, for tests. Builds with the
// disable_trap tag leave it out.
func SetObserverCommitAdvertisementDelay(delay time.Duration) {
	observerCommitAdvertisementDelay.Store(int64(delay))
}

// holdBackCommitAdvertisement waits for up to the observer commit advertisement
// delay, or until an entry past currentOffset is written.
func holdBackCommitAdvertisement(ctx context.Context, ackTracker QuorumAckTracker, currentOffset int64) error {
	delay := time.Duration(observerCommitAdvertisementDelay.Load())
	if delay <= 0 {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, delay)
	defer cancel()
	_ = ackTracker.WaitForHeadOffset(waitCtx, currentOffset+1)
	return ctx.Err()
}
