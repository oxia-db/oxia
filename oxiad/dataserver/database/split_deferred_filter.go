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
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// The bounds of a step of the deferred split filter (see SplitFilterKeys).
// Vars, not consts, so tests can shrink them.
var (
	splitFilterMaxKeys  = 1000
	splitFilterMaxBytes = 1024 * 1024
)

// DeferredSplitFilter is the filter of a split child seeded with all the data
// of its parent, in the parent's term ParentTerm: the child keeps the records
// whose hash is in [MinHash, MaxHash], and deletes the others once the split
// completes, through its log (see SplitFilterKeys). Until then:
//   - The child applies the entries of its parent, of the terms up to
//     ParentTerm, as the parent applied them (see DB.ProcessSplitParentWrite):
//     their outcome, like the version ids they assign, can depend on the
//     records of the other child.
//   - Its reads skip the records it doesn't keep, and so do the writes of its
//     own terms, for which the records don't exist: they drop one before they
//     touch it.
//
// Only a split child holds such a filter, until it deleted those records: none
// of what it does runs on any other shard.
type DeferredSplitFilter struct {
	MinHash    uint32
	MaxHash    uint32
	ParentTerm int64
}

// keeps reports whether the child keeps the record at key, whose entry is se:
// the hash of its partition key places it, if it has one, or the hash of its
// key, as the clients route their requests.
func (f *DeferredSplitFilter) keeps(key string, se *proto.StorageEntry) bool {
	var h uint32
	if se.PartitionKey != nil {
		h = hash.Xxh332(*se.PartitionKey)
	} else {
		h = hash.Xxh332(key)
	}
	return h >= f.MinHash && h <= f.MaxHash
}

// keepsValue reports whether the child keeps the record at key, whose entry is
// value. An internal key isn't a record, and is kept. A record that can't be
// deserialized is placed by its key, as FilterDBForSplit places it.
func (f *DeferredSplitFilter) keepsValue(key string, value []byte) bool {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return true
	}
	se := proto.StorageEntryFromVTPool()
	defer se.ReturnToVTPool()
	if err := DeserializeMetadata(value, se); err != nil {
		se.ResetVT()
	}
	return f.keeps(key, se)
}

// SetDeferredSplitFilter records the deferred filter of a split child, seeded
// with a snapshot of its parent, and flushes the database, which runs without
// the Pebble WAL: once the child acks the snapshot, the parent doesn't send it
// again. A parent that is itself the child of an earlier split, seeded with a
// filtered snapshot, has the SplitFilter of that split, which this child drops:
// the child gets no entry of the terms it covers.
//
// The child also records the notifications it delivers of the batches it
// inherits, with the range of the parent, parentHashRange, if known (see
// inheritedNotifications).
func (d *db) SetDeferredSplitFilter(filter *DeferredSplitFilter, parentHashRange *proto.HashRange) error {
	value, err := json.Marshal(filter)
	if err != nil {
		return err
	}
	var inherited inheritedNotifications
	if parentInherited := d.inheritedNotifications.Load(); parentInherited != nil {
		inherited = *parentInherited
	}
	inherited = inherited.withSplit(splitNotificationsRange(
		&proto.HashRange{Min: filter.MinHash, Max: filter.MaxHash}, parentHashRange))

	batch := d.kv.NewWriteBatch()
	defer batch.Close()
	if err := d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:   deferredSplitFilterKey,
		Value: value,
	}, now(), NoOpCallback, true, nil, nil); err != nil {
		return err
	}
	if err := d.putInheritedNotifications(batch, inherited); err != nil {
		return err
	}
	if err := batch.Delete(splitFilterKey); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	if err := d.kv.Flush(); err != nil {
		return err
	}
	d.splitFilter.Store(nil)
	d.deferredSplitFilter.Store(filter)
	d.inheritedNotifications.Store(&inherited)
	return nil
}

func (d *db) DeferredSplitFilter() *DeferredSplitFilter {
	return d.deferredSplitFilter.Load()
}

func (d *db) recoverDeferredSplitFilter() error {
	gr, err := applyGet(d.kv, &proto.GetRequest{Key: deferredSplitFilterKey, IncludeValue: true})
	if err != nil {
		return err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return nil
	}

	filter := &DeferredSplitFilter{}
	if err := json.Unmarshal(gr.Value, filter); err != nil {
		return errors.Wrap(err, "invalid deferred split filter")
	}
	d.deferredSplitFilter.Store(filter)
	return nil
}

// ProcessSplitParentWrite applies a write request of the parent of a split
// child as the parent applied it, with the records of the other child (see
// DeferredSplitFilter).
func (d *db) ProcessSplitParentWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64,
	updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error) {
	return d.processWrite(b, commitOffset, timestamp, updateOperationCallback, nil)
}

// dropFilteredRecords drops the records that the child doesn't keep at the keys
// of the puts and the deletes of a write of the child's own terms, before it
// applies: the write must not find them. The write can't add any such record:
// the clients send the child the requests on its records only.
func (d *db) dropFilteredRecords(batch kvstore.WriteBatch, b *proto.WriteRequest, filter *DeferredSplitFilter,
	updateOperationCallback UpdateOperationCallback) error {
	for _, put := range b.Puts {
		if err := d.dropFilteredRecord(batch, put.Key, filter, updateOperationCallback); err != nil {
			return err
		}
	}
	for _, del := range b.Deletes {
		if err := d.dropFilteredRecord(batch, del.Key, filter, updateOperationCallback); err != nil {
			return err
		}
	}
	return nil
}

// dropFilteredRecord deletes the record at key if the child doesn't keep it,
// with its secondary index entries and its session shadow key, and without a
// notification: for the child, the record doesn't exist.
func (d *db) dropFilteredRecord(batch kvstore.WriteBatch, key string, filter *DeferredSplitFilter,
	updateOperationCallback UpdateOperationCallback) error {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return nil
	}
	se, err := GetStorageEntryMetadata(batch, key)
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer se.ReturnToVTPool()
	if filter.keeps(key, se) {
		return nil
	}
	if err := updateOperationCallback.OnDeleteWithEntry(batch, nil, key, se, d); err != nil {
		return err
	}
	return batch.Delete(key)
}

// notifiedDeletion returns the filter of the deletions that a write of the
// child's own terms notifies, or nil without a deferred split filter: it
// doesn't notify the deletion of a record that the child doesn't keep, like an
// ephemeral record of the other child when their session ends. The record is
// read from the batch, where it must still be: a deletion recorded once the
// record is gone is notified.
func notifiedDeletion(batch kvstore.WriteBatch, filter *DeferredSplitFilter) func(key string) bool {
	if filter == nil {
		return nil
	}
	return func(key string) bool {
		se, err := GetStorageEntryMetadata(batch, key)
		if err != nil {
			return true
		}
		defer se.ReturnToVTPool()
		return filter.keeps(key, se)
	}
}

// applySplitFilter applies a step of the deferred split filter: it drops the
// records at the keys of the request that the child doesn't keep, and with the
// last step, the filter itself. It reports whether the filter completed.
func (d *db) applySplitFilter(batch kvstore.WriteBatch, req *proto.SplitFilterRequest,
	updateOperationCallback UpdateOperationCallback) (bool, error) {
	filter := d.deferredSplitFilter.Load()
	if filter == nil {
		// The filter completed already, e.g. with the steps of an earlier
		// leader of the child
		return false, nil
	}
	for _, key := range req.Keys {
		if err := d.dropFilteredRecord(batch, key, filter, updateOperationCallback); err != nil {
			return false, err
		}
	}
	if !req.Complete {
		return false, nil
	}
	return true, batch.Delete(deferredSplitFilterKey)
}

// SplitFilterKeys returns, in the order of the shard, the keys of the records
// that the deferred split filter drops, after the key after, or from the first
// record if it's nil: the keys of a step of the filter, up to splitFilterMaxKeys
// of them and splitFilterMaxBytes bytes. complete reports that no such record
// follows them.
func (d *db) SplitFilterKeys(after *string) (keys []string, complete bool, err error) {
	filter := d.deferredSplitFilter.Load()
	if filter == nil {
		return nil, true, nil
	}

	start := ""
	if after != nil {
		start = *after
	}
	it, err := d.kv.RangeScan(start, "", kvstore.NoInternalKeys)
	if err != nil {
		return nil, false, err
	}
	if after != nil && it.Valid() && it.Key() == *after {
		it.Next()
	}

	size := 0
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value, err := it.Value()
		if err != nil {
			return nil, false, multierr.Append(err, it.Close())
		}
		if filter.keepsValue(key, value) {
			continue
		}
		keys = append(keys, key)
		size += len(key)
		if len(keys) >= splitFilterMaxKeys || size >= splitFilterMaxBytes {
			return keys, false, it.Close()
		}
	}

	// The iteration also stops when a read fails
	err = multierr.Append(it.Error(), it.Close())
	return keys, err == nil, err
}

// getKept is Get on a split child that holds records it doesn't keep: the
// record it returns is the closest to the key of the request, by its
// comparison, among the ones the child keeps.
func (d *db) getKept(getReq *proto.GetRequest, filter *DeferredSplitFilter) (*proto.GetResponse, error) {
	switch getReq.ComparisonType {
	case proto.KeyComparisonType_EQUAL:
		return d.getKeptEqual(getReq, filter)
	case proto.KeyComparisonType_FLOOR:
		// The record at the key, or the closest one below it
		if res, err := d.getKeptEqual(getReq, filter); err != nil || res.Status == proto.Status_OK {
			return res, err
		}
		return d.getKeptBelow(getReq, filter)
	case proto.KeyComparisonType_LOWER:
		return d.getKeptBelow(getReq, filter)
	case proto.KeyComparisonType_CEILING, proto.KeyComparisonType_HIGHER:
		return d.getKeptAbove(getReq, filter)
	default:
		return nil, errors.Errorf("unsupported comparison type: %v", getReq.ComparisonType)
	}
}

func (d *db) getKeptEqual(getReq *proto.GetRequest, filter *DeferredSplitFilter) (*proto.GetResponse, error) {
	key, value, closer, err := d.kv.Get(getReq.Key, kvstore.ComparisonEqual, kvstore.NoInternalKeys)
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	var res *proto.GetResponse
	if filter.keepsValue(key, value) {
		res, err = newGetResponse(getReq, key, value)
	} else {
		res = &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}
	}
	if err = multierr.Append(err, closer.Close()); err != nil {
		return nil, err
	}
	return res, nil
}

// getKeptBelow returns the closest record below the key of the request that
// the child keeps.
func (d *db) getKeptBelow(getReq *proto.GetRequest, filter *DeferredSplitFilter) (*proto.GetResponse, error) {
	if getReq.Key == "" {
		// Nothing sorts below the empty key
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	}
	it, err := d.kv.RangeScan("", getReq.Key, kvstore.NoInternalKeys)
	if err != nil {
		return nil, err
	}
	it.SeekLT(getReq.Key)
	return getKeptClosest(it, getReq, filter, it.Prev)
}

// getKeptAbove returns the closest record above the key of the request, or at
// it for a CEILING, that the child keeps.
func (d *db) getKeptAbove(getReq *proto.GetRequest, filter *DeferredSplitFilter) (*proto.GetResponse, error) {
	it, err := d.kv.RangeScan(getReq.Key, "", kvstore.NoInternalKeys)
	if err != nil {
		return nil, err
	}
	if getReq.ComparisonType == proto.KeyComparisonType_HIGHER && it.Valid() && it.Key() == getReq.Key {
		it.Next()
	}
	return getKeptClosest(it, getReq, filter, it.Next)
}

// getKeptClosest returns the first record that the child keeps, from the
// position of it on, moving it with next.
func getKeptClosest(it kvstore.KeyValueIterator, getReq *proto.GetRequest, filter *DeferredSplitFilter,
	next func() bool) (*proto.GetResponse, error) {
	for ; it.Valid(); next() {
		key := it.Key()
		value, err := it.Value()
		if err != nil {
			return nil, multierr.Append(err, it.Close())
		}
		if filter.keepsValue(key, value) {
			res, err := newGetResponse(getReq, key, value)
			if err = multierr.Append(err, it.Close()); err != nil {
				return nil, err
			}
			return res, nil
		}
	}

	// The iteration also stops when a read fails
	if err := multierr.Append(it.Error(), it.Close()); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}
	return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
}

// keptRecordsIterator is the iterator of a List or RangeScan on a split child
// that holds records it doesn't keep: it skips them. It reads the entry of
// each record, which a List otherwise doesn't.
type keptRecordsIterator struct {
	kvstore.KeyValueIterator
	filter *DeferredSplitFilter
	err    error
}

func newKeptRecordsIterator(it kvstore.KeyValueIterator, filter *DeferredSplitFilter) *keptRecordsIterator {
	kept := &keptRecordsIterator{KeyValueIterator: it, filter: filter}
	kept.skip(it.Next)
	return kept
}

// skip moves the iterator with next until it reaches a record that the child
// keeps, and reports whether it did.
func (it *keptRecordsIterator) skip(next func() bool) bool {
	for it.KeyValueIterator.Valid() {
		value, err := it.Value()
		if err != nil {
			it.err = err
			return false
		}
		if it.filter.keepsValue(it.Key(), value) {
			return true
		}
		next()
	}
	return false
}

func (it *keptRecordsIterator) Valid() bool {
	return it.err == nil && it.KeyValueIterator.Valid()
}

func (it *keptRecordsIterator) Next() bool {
	it.KeyValueIterator.Next()
	return it.skip(it.KeyValueIterator.Next)
}

func (it *keptRecordsIterator) Prev() bool {
	it.KeyValueIterator.Prev()
	return it.skip(it.KeyValueIterator.Prev)
}

func (it *keptRecordsIterator) SeekGE(key string) bool {
	it.KeyValueIterator.SeekGE(key)
	return it.skip(it.KeyValueIterator.Next)
}

func (it *keptRecordsIterator) SeekLT(key string) bool {
	it.KeyValueIterator.SeekLT(key)
	return it.skip(it.KeyValueIterator.Prev)
}

func (it *keptRecordsIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.KeyValueIterator.Error()
}

func (it *keptRecordsIterator) Close() error {
	return multierr.Append(it.err, it.KeyValueIterator.Close())
}
