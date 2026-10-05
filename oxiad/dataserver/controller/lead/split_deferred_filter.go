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
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/statemachine"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// What a split child seeded with all the data of its parent does until it
// deletes the records outside its hash range (see database.DeferredSplitFilter).
// None of it runs on a shard that doesn't hold such records.

// startDeferredSplitFilter starts deleting, in the background, the records that
// the shard holds outside its hash range since its split, if any, while the
// node leads the term, termCtx. The caller must hold the leader write lock, as
// becomeLeader does.
func (lc *leaderController) startDeferredSplitFilter(termCtx context.Context, term int64) {
	if lc.db.DeferredSplitFilter() == nil {
		return
	}
	if !lc.db.IsFeatureEnabled(proto.Feature_FEATURE_SPLIT_DEFERRED_FILTER) {
		// Only the members that support the feature can apply the steps of the
		// filter. The child of a split holds the records of its parent only
		// when the term of the parent pins it, which enables it on the parent,
		// and on the child with the parent's entries.
		lc.log.Error(
			"The shard holds records outside its hash range, but can't delete them without the feature",
			slog.Int64("term", term),
			slog.Any("feature", proto.Feature_FEATURE_SPLIT_DEFERRED_FILTER),
		)
		return
	}

	commitOffset := lc.dbCommitOffset.Load()
	go process.DoWithLabels(termCtx, map[string]string{
		"oxia":      "split-deferred-filter",
		"namespace": lc.namespace,
		"shard":     fmt.Sprintf("%d", lc.shardId),
	}, func() {
		if lc.waitForSplitFilterQuorum(termCtx, term, commitOffset) {
			lc.applyDeferredSplitFilter(termCtx, term)
		}
	})
}

// splitFilterQuorumCheckInterval is how often the leader checks whether a
// majority of the shard holds its entries, see waitForSplitFilterQuorum.
const splitFilterQuorumCheckInterval = 100 * time.Millisecond

// waitForSplitFilterQuorum waits until a majority of the shard, the leader
// included, holds the entries up to offset, those the leader had when elected:
// a step of the deferred split filter that can't be committed would hold back
// the next leader election of the shard, which must commit it, e.g. while
// followers that missed the split install a snapshot of the child. It reports
// whether the node still leads the term.
func (lc *leaderController) waitForSplitFilterQuorum(ctx context.Context, term int64, offset int64) bool {
	ticker := time.NewTicker(splitFilterQuorumCheckInterval)
	defer ticker.Stop()
	for {
		held, leading := lc.splitFilterQuorum(term, offset)
		if held || !leading {
			return leading
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// splitFilterQuorum reports whether a majority of the shard holds the entries
// up to offset, and whether the node still leads the term.
func (lc *leaderController) splitFilterQuorum(term int64, offset int64) (held bool, leading bool) {
	lc.RLock()
	defer lc.RUnlock()
	if lc.status != proto.ServingStatus_LEADER || lc.term.Load() != term {
		return false, false
	}
	holders := 1
	for _, follower := range lc.followers {
		if follower.AckOffset() >= offset {
			holders++
		}
	}
	return holders > int(lc.replicationFactor)/2, true
}

// applyDeferredSplitFilter deletes, through the log, the records that the shard
// holds outside its hash range since its split: each step deletes a batch of
// them, and the next one starts once it's applied. The reads of the shard, and
// its writes, skip these records until then. It stops when the node stops
// leading the term, ctx: the leader of a later term starts over.
func (lc *leaderController) applyDeferredSplitFilter(ctx context.Context, term int64) {
	log := lc.log.With(slog.Int64("term", term))
	log.Info("Deleting the records outside the hash range of the shard, held since its split")

	bo := time2.NewBackOff(ctx)
	var after *string
	deleted := 0
	for {
		keys, complete, err := lc.splitFilterStep(ctx, term, after)
		if err != nil {
			retryAfter := bo.NextBackOff()
			if retryAfter == backoff.Stop {
				log.Info("Stopped deleting the records outside the hash range of the shard", slog.Any("error", err))
				return
			}
			log.Warn(
				"Failed to delete records outside the hash range of the shard, retrying",
				slog.Any("error", err),
				slog.Duration("retry-after", retryAfter),
			)
			select {
			case <-ctx.Done():
			case <-time.After(retryAfter):
			}
			continue
		}
		bo.Reset()

		deleted += len(keys)
		if complete {
			log.Info("Deleted the records outside the hash range of the shard", slog.Int("count", deleted))
			return
		}
		after = &keys[len(keys)-1]
	}
}

// splitFilterStep proposes the next step of the deferred split filter, that
// deletes the records after the key after, and waits for it to be applied. It
// returns the keys of the records, and whether it was the last step.
func (lc *leaderController) splitFilterStep(ctx context.Context, term int64, after *string) ([]string, bool, error) {
	keys, complete, err := lc.splitFilterKeys(term, after)
	if err != nil {
		return nil, false, err
	}
	_, err = lc.proposeBlock(ctx, ctx.Done(), func(offset int64) statemachine.Proposal {
		return statemachine.NewControlProposal(offset, &proto.ControlRequest{
			Value: &proto.ControlRequest_SplitFilter{
				SplitFilter: &proto.SplitFilterRequest{Keys: keys, Complete: complete},
			},
		})
	})
	return keys, complete, err
}

// splitFilterKeys reads the keys of the next step of the deferred split filter,
// like a read: only while the node leads the term, and before close can close
// the database.
func (lc *leaderController) splitFilterKeys(term int64, after *string) ([]string, bool, error) {
	lc.RLock()
	if lc.status != proto.ServingStatus_LEADER || lc.term.Load() != term {
		lc.RUnlock()
		return nil, false, constant.ErrNodeIsNotLeader
	}
	lc.waitGroup.Add(1)
	lc.RUnlock()
	defer lc.waitGroup.Done()

	return lc.db.SplitFilterKeys(after)
}

// errSplitFilterPending rejects a split of a shard that still holds records of
// its own split outside its hash range: the children would apply the steps of
// its deferred split filter, which delete these records, with their own filter,
// and keep different records than the shard. The split can start once the
// filter completed.
var errSplitFilterPending = errors.Wrap(constant.ErrResourceUnavailable,
	"the shard still holds records outside its hash range since its split")

// keptIndexEntries returns an iterator over the entries of a secondary index,
// it, that skips the entries of the records that the shard holds but doesn't
// keep: the records of a split child outside its hash range, until it deletes
// them. On any other shard, it returns it.
func keptIndexEntries(db database.DB, it kvstore.KeyIterator) kvstore.KeyIterator {
	if db.DeferredSplitFilter() == nil {
		return it
	}
	return &keptIndexEntriesIterator{KeyIterator: it, db: db}
}

type keptIndexEntriesIterator struct {
	kvstore.KeyIterator
	db  database.DB
	err error
}

// skip moves the iterator with next until it reaches an entry of a record that
// the shard keeps, and reports whether it did. An entry that can't be parsed
// is kept: the reads of the index handle it.
func (it *keptIndexEntriesIterator) skip(next func() bool) bool {
	for it.KeyIterator.Valid() {
		primaryKey, _, err := database.ParseSecondaryIndexKey(it.Key())
		if err != nil {
			return true
		}
		// The Get of a split child doesn't find a record it doesn't keep
		res, err := it.db.Get(&proto.GetRequest{Key: primaryKey})
		if err != nil {
			it.err = err
			return false
		}
		if res.Status == proto.Status_OK {
			return true
		}
		next()
	}
	return false
}

func (it *keptIndexEntriesIterator) Valid() bool {
	return it.err == nil && it.KeyIterator.Valid()
}

func (it *keptIndexEntriesIterator) Next() bool {
	it.KeyIterator.Next()
	return it.skip(it.KeyIterator.Next)
}

func (it *keptIndexEntriesIterator) Prev() bool {
	it.KeyIterator.Prev()
	return it.skip(it.KeyIterator.Prev)
}

func (it *keptIndexEntriesIterator) SeekGE(key string) bool {
	it.KeyIterator.SeekGE(key)
	return it.skip(it.KeyIterator.Next)
}

func (it *keptIndexEntriesIterator) SeekLT(key string) bool {
	it.KeyIterator.SeekLT(key)
	return it.skip(it.KeyIterator.Prev)
}

func (it *keptIndexEntriesIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.KeyIterator.Error()
}

func (it *keptIndexEntriesIterator) Close() error {
	return multierr.Append(it.err, it.KeyIterator.Close())
}
