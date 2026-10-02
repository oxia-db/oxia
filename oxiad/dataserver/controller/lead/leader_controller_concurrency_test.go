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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// ackedKeys collects the keys whose write was acknowledged.
type ackedKeys struct {
	sync.Mutex
	keys []string
}

func (a *ackedKeys) add(key string) {
	a.Lock()
	a.keys = append(a.keys, key)
	a.Unlock()
}

func (a *ackedKeys) random() (string, bool) {
	a.Lock()
	defer a.Unlock()
	if len(a.keys) == 0 {
		return "", false
	}
	return a.keys[rand.IntN(len(a.keys))], true
}

// The proposals hold only the leader read lock: the reads, the writes and the
// session writes run concurrently, while the new terms and the freezes still
// exclude the proposals in progress. Every acknowledged write must stay
// readable, the head must not move while frozen, and no entry of an older term
// may land after the head entry that a new term found.
func TestLeaderController_ConcurrentProposalsAndStateChanges(t *testing.T) {
	for _, syncData := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync-data=%v", syncData), func(t *testing.T) {
			testConcurrentProposalsAndStateChanges(t, syncData)
		})
	}
}

func testConcurrentProposalsAndStateChanges(t *testing.T, syncData bool) {
	t.Helper()
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
		// Small segments, so that the appends also roll over to new segments
		SegmentSize: 128 * 1024,
		SyncData:    syncData,
		Retention:   time.Hour,
	})
	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(),
		walFactory, kvFactory, nil)
	require.NoError(t, err)

	term := int64(1)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: term})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard: shard, Term: term, ReplicationFactor: 1})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	acked := &ackedKeys{}

	// A write can only fail because the leader is fenced or frozen, or because
	// its term ended before it was applied
	checkWriteError := func(err error) {
		if !errors.Is(err, constant.ErrNodeIsNotLeader) && !errors.Is(err, constant.ErrResourceUnavailable) {
			assert.Fail(t, "unexpected write error", "%+v", err)
		}
	}

	// Async writers, with up to 64 writes in flight each
	for w := range 4 {
		wg.Go(func() {
			inflight := make(chan struct{}, 64)
			var failed atomic.Bool
			for i := 0; ; i++ {
				if failed.Swap(false) {
					time.Sleep(time.Millisecond)
				}
				select {
				case inflight <- struct{}{}:
				case <-ctx.Done():
					// Wait for the writes in flight
					for range cap(inflight) {
						inflight <- struct{}{}
					}
					return
				}
				key := fmt.Sprintf("async-%d-%07d", w, i)
				lc.Write(context.Background(), &proto.WriteRequest{
					Shard: &shard,
					Puts:  []*proto.PutRequest{{Key: key, Value: []byte(key)}},
				}, concurrent.NewOnce(func(res *proto.WriteResponse) {
					assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
					acked.add(key)
					<-inflight
				}, func(err error) {
					checkWriteError(err)
					failed.Store(true)
					<-inflight
				}))
			}
		})
	}

	// Blocking writers
	for w := range 2 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				key := fmt.Sprintf("block-%d-%07d", w, i)
				res, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
					Shard: &shard,
					Puts:  []*proto.PutRequest{{Key: key, Value: []byte(key)}},
				})
				if err != nil {
					checkWriteError(err)
					time.Sleep(time.Millisecond)
					continue
				}
				assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
				acked.add(key)
			}
		})
	}

	// The session manager writes through writeBlock, with its own done channel.
	// The test reads the manager under the leader lock: lc.CreateSession reads
	// it without, racing with BecomeLeader, which replaces it.
	sessionManager := func() SessionManager {
		lc.(*leaderController).RLock()
		defer lc.(*leaderController).RUnlock()
		return lc.(*leaderController).sessionManager
	}
	wg.Go(func() {
		for ctx.Err() == nil {
			res, err := sessionManager().CreateSession(&proto.CreateSessionRequest{
				Shard:            shard,
				SessionTimeoutMs: uint32(constant.MinSessionTimeout.Milliseconds()),
			})
			if err != nil {
				checkWriteError(err)
				time.Sleep(time.Millisecond)
				continue
			}
			_, err = sessionManager().CloseSession(&proto.CloseSessionRequest{Shard: shard, SessionId: res.SessionId})
			if err != nil && !errors.Is(err, constant.ErrSessionNotFound) {
				checkWriteError(err)
			}
		}
	})

	// Readers: every read that succeeds sees the acknowledged writes
	for range 4 {
		wg.Go(func() {
			for ctx.Err() == nil {
				key, ok := acked.random()
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				results, err := readAll(context.Background(), lc, &proto.ReadRequest{
					Shard: &shard,
					Gets:  []*proto.GetRequest{{Key: key, IncludeValue: true}},
				})
				if err != nil {
					assert.ErrorIs(t, err, constant.ErrNodeIsNotLeader)
					continue
				}
				if assert.Len(t, results, 1) {
					assert.Equal(t, proto.Status_OK, results[0].Status, "key %s", key)
					assert.Equal(t, []byte(key), results[0].Value, "key %s", key)
				}
			}
		})
	}
	wg.Go(func() {
		for ctx.Err() == nil {
			_, err := lc.GetStatus(&proto.GetStatusRequest{Shard: shard})
			assert.NoError(t, err)
		}
	})

	// New terms and freezes, meanwhile. Every new term records the head entry
	// it found.
	newTermHeads := map[int64]*proto.EntryId{}
	wg.Go(func() {
		for ctx.Err() == nil {
			time.Sleep(time.Duration(rand.IntN(20)) * time.Millisecond)
			if rand.IntN(2) == 0 {
				fr, err := lc.Freeze(&proto.FreezeShardRequest{Shard: shard, Term: term, Frozen: true})
				if !assert.NoError(t, err) {
					return
				}
				time.Sleep(time.Millisecond)
				st, err := lc.GetStatus(&proto.GetStatusRequest{Shard: shard})
				assert.NoError(t, err)
				assert.Equal(t, fr.HeadOffset, st.HeadOffset, "the head moved while frozen")
				_, err = lc.Freeze(&proto.FreezeShardRequest{Shard: shard, Term: term, Frozen: false})
				if !assert.NoError(t, err) {
					return
				}
			}

			term++
			res, err := lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: term})
			if !assert.NoError(t, err) {
				return
			}
			newTermHeads[term] = res.HeadEntryId
			_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
				Shard: shard, Term: term, ReplicationFactor: 1})
			if !assert.NoError(t, err) {
				return
			}
		}
	})

	stopped := make(chan struct{})
	go func() {
		wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the goroutines did not stop")
	}
	require.False(t, t.Failed())

	// Every acknowledged write is still readable
	keys := acked.keys
	require.NotEmpty(t, keys)
	for start := 0; start < len(keys); start += 1000 {
		gets := make([]*proto.GetRequest, 0, 1000)
		for _, key := range keys[start:min(start+1000, len(keys))] {
			gets = append(gets, &proto.GetRequest{Key: key, IncludeValue: true})
		}
		results, err := readAll(context.Background(), lc, &proto.ReadRequest{Shard: &shard, Gets: gets})
		require.NoError(t, err)
		for i, r := range results {
			require.Equal(t, proto.Status_OK, r.Status, "key %s", gets[i].Key)
			require.Equal(t, []byte(gets[i].Key), r.Value)
		}
	}

	// The wal has contiguous offsets, its terms never go back, and no entry
	// of an older term is after the head entry that a new term found
	r, err := lc.(*leaderController).wal.NewReader(wal.InvalidOffset)
	require.NoError(t, err)
	expectedOffset := int64(0)
	lastTerm := wal.InvalidTerm
	for r.HasNext() {
		entry, _, _, err := r.ReadNext()
		require.NoError(t, err)
		require.Equal(t, expectedOffset, entry.Offset)
		require.GreaterOrEqual(t, entry.Term, lastTerm, "offset %d", entry.Offset)
		for newTerm, head := range newTermHeads {
			if entry.Offset > head.Offset {
				require.GreaterOrEqual(t, entry.Term, newTerm,
					"the entry %d of term %d is after the head %v that term %d found", entry.Offset, entry.Term, head, newTerm)
			}
		}
		expectedOffset++
		lastTerm = entry.Term
	}
	require.NoError(t, r.Close())
	t.Logf("%d terms, %d acknowledged writes, %d wal entries", len(newTermHeads), len(keys), expectedOffset)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
