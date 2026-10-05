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
	"encoding/json"
	"math"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

const (
	inheritedNotificationsKey = constant.InternalKeyPrefix + "split-notifications"

	// The LastOffset of the batches that a split child inherits at a split,
	// until it writes a batch of its own: every batch it holds until then is
	// inherited.
	inheritedNotificationsOpen = math.MaxInt64
)

// inheritedNotifications are the notification batches that a split child
// copied from its parent (see DeferredSplitFilter), which the other child
// copied as well. Of those, the child delivers the notifications whose key hash
// is on its side of the split, so that a subscriber that resumes on both
// children, from the last offset it read from their parent, gets each of them
// once. There is one entry per split the child descends from, in their order: a
// batch was inherited at the first split whose LastOffset is not below its
// offset, and the range of that split, which every later split narrowed, holds
// the notifications that the child delivers of it. The batches of the child's
// own writes follow the last split, and the child delivers them all.
type inheritedNotifications []inheritedNotificationsSplit

type inheritedNotificationsSplit struct {
	// LastOffset is the offset of the last batch that the child inherited at
	// the split, or inheritedNotificationsOpen
	LastOffset int64
	// The range of the key hashes of the notifications that the child
	// delivers, of the batches it inherited at the split
	MinHash uint32
	MaxHash uint32
}

// splitNotificationsRange returns the range of the key hashes of the
// notifications that a split child delivers, of the batches it inherits from
// its parent: its own range, extended to the bound of the hash space on each
// side where it reaches the bound of the range of the parent. The notification
// of a record placed in the parent by its partition key can have a key hash
// outside the range of the parent: it goes to the child on that side.
//
// Without the range of the parent, a child that reaches a bound of the hash
// space knows that it's on that side of the split, and delivers its own range.
// Any other child delivers all the notifications: the subscriber gets those of
// its range twice, rather than miss the others.
func splitNotificationsRange(childRange *proto.HashRange, parentRange *proto.HashRange) (
	minHash uint32, maxHash uint32) {
	minHash, maxHash = childRange.GetMin(), childRange.GetMax()
	if parentRange == nil {
		if minHash == 0 || maxHash == math.MaxUint32 {
			return minHash, maxHash
		}
		return 0, math.MaxUint32
	}
	if minHash == parentRange.GetMin() {
		minHash = 0
	}
	if maxHash == parentRange.GetMax() {
		maxHash = math.MaxUint32
	}
	return minHash, maxHash
}

// withSplit returns the inherited notifications of a child of a shard that has
// inherited, at a new split whose range of the notifications that the child
// delivers is [minHash, maxHash]: the child delivers, of the batches that its
// parent inherited, those in the range of the parent that are in this range as
// well, and of the batches of the parent, those in this range, up to the batch
// it writes first.
func (in inheritedNotifications) withSplit(minHash uint32, maxHash uint32) inheritedNotifications {
	res := make(inheritedNotifications, 0, len(in)+1)
	for _, split := range in {
		res = append(res, inheritedNotificationsSplit{
			LastOffset: split.LastOffset,
			MinHash:    max(split.MinHash, minHash),
			MaxHash:    min(split.MaxHash, maxHash),
		})
	}
	return append(res, inheritedNotificationsSplit{
		LastOffset: inheritedNotificationsOpen,
		MinHash:    minHash,
		MaxHash:    maxHash,
	})
}

// open reports whether the child hasn't written a batch of its own since the
// last split.
func (in inheritedNotifications) open() bool {
	return len(in) > 0 && in[len(in)-1].LastOffset == inheritedNotificationsOpen
}

// closedAt returns the inherited notifications of a child whose batches of the
// last split end at lastOffset.
func (in inheritedNotifications) closedAt(lastOffset int64) inheritedNotifications {
	res := append(inheritedNotifications(nil), in...)
	res[len(res)-1].LastOffset = lastOffset
	return res
}

// filter returns the batches with the notifications that the child delivers:
// it filters the ones it inherited.
func (in inheritedNotifications) filter(batches []proto.EncodedNotificationBatch) (
	[]proto.EncodedNotificationBatch, error) {
	if len(in) == 0 || len(batches) == 0 || batches[0].Offset > in[len(in)-1].LastOffset {
		return batches, nil
	}

	// The batches can be shared with the notifications cache: the filtered ones
	// are new
	res := make([]proto.EncodedNotificationBatch, len(batches))
	split := 0
	for i, batch := range batches {
		for split < len(in) && in[split].LastOffset < batch.Offset {
			split++
		}
		if split == len(in) {
			res[i] = batch
			continue
		}
		filtered, err := filterNotificationBatch(batch, in[split].MinHash, in[split].MaxHash)
		if err != nil {
			return nil, err
		}
		res[i] = filtered
	}
	return res, nil
}

// filterNotificationBatch returns the batch with the notifications whose key
// hash is in [minHash, maxHash]. A batch left without notifications is still
// returned: the subscriber moves past its offset.
func filterNotificationBatch(batch proto.EncodedNotificationBatch, minHash uint32, maxHash uint32) (
	proto.EncodedNotificationBatch, error) {
	nb := &proto.NotificationBatch{}
	if err := nb.UnmarshalVT(batch.Data); err != nil {
		return proto.EncodedNotificationBatch{}, errors.Wrap(err, "failed to deserialize notification batch")
	}
	kept := nb.Notifications[:0]
	for _, entry := range nb.Notifications {
		if h := hash.Xxh332(entry.GetKey()); h >= minHash && h <= maxHash {
			kept = append(kept, entry)
		}
	}
	if len(kept) == len(nb.Notifications) {
		return batch, nil
	}
	nb.Notifications = kept
	data, err := nb.MarshalVT()
	if err != nil {
		return proto.EncodedNotificationBatch{}, err
	}
	return proto.EncodedNotificationBatch{Offset: batch.Offset, Data: data}, nil
}

// putInheritedNotifications adds the write of the inherited notifications to the
// batch.
func (d *db) putInheritedNotifications(batch kvstore.WriteBatch, in inheritedNotifications) error {
	value, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:   inheritedNotificationsKey,
		Value: value,
	}, now(), NoOpCallback, true, nil, nil)
}

// closeInheritedNotifications records that the batches that the child inherited
// at its last split end before the entry at offset, which is of the child's
// own terms, unless it has recorded it already.
func (d *db) closeInheritedNotifications(offset int64) error {
	in := d.inheritedNotifications.Load()
	if in == nil || !in.open() {
		return nil
	}
	closed := in.closedAt(offset - 1)

	// In a batch of its own, before the one of the entry: the batches of the
	// entries feed the checksum, while a replica can also get the inherited
	// notifications closed already, from a snapshot. Recorded before the batch
	// of the entry is written, so that a reader that sees the batch also finds
	// it out of the inherited ones.
	batch := d.kv.NewWriteBatch()
	defer batch.Close()
	if err := d.putInheritedNotifications(batch, closed); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	d.inheritedNotifications.Store(&closed)
	return nil
}

// closeInheritedNotificationsWithFilter adds to the batch of the last step of
// the deferred split filter, at offset, the record that the batches that the
// child inherited end before it, if the child hasn't written a batch of its own
// yet: once the filter completes, the writes of the child no longer record it.
// It returns the inherited notifications to store once the batch is
// committed, or nil.
func (d *db) closeInheritedNotificationsWithFilter(batch kvstore.WriteBatch, offset int64) (
	inheritedNotifications, error) {
	in := d.inheritedNotifications.Load()
	if in == nil || !in.open() {
		return nil, nil
	}
	closed := in.closedAt(offset - 1)
	return closed, d.putInheritedNotifications(batch, closed)
}

func (d *db) recoverInheritedNotifications() error {
	gr, err := applyGet(d.kv, &proto.GetRequest{Key: inheritedNotificationsKey, IncludeValue: true})
	if err != nil {
		return err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return nil
	}

	var in inheritedNotifications
	if err := json.Unmarshal(gr.Value, &in); err != nil {
		return errors.Wrap(err, "invalid inherited notifications")
	}
	d.inheritedNotifications.Store(&in)
	return nil
}
