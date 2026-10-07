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
	"net/url"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// DeferredSplitFilter is the filter of a split child seeded with all the data
// of its parent, in the parent's term ParentTerm: the child keeps the records
// whose hash is in [MinHash, MaxHash], and deletes the others once the split
// completes, through its log, in steps (see proto.SplitFilterRequest). Until
// then:
//   - The child applies the entries of its parent, of the terms up to
//     ParentTerm, as the parent applied them (see DB.ProcessSplitParentWrite):
//     their outcome, like the version ids they assign, can depend on the
//     records of the other child.
//   - Its reads skip the records it doesn't keep, and so do the writes of its
//     own terms, for which the records don't exist: they drop one they find.
//
// Cursor is where the next step starts, as a key of the store (see
// kvstore.KV.Scan): the child keeps all the records before it.
//
// Only a split child holds such a filter, until it deleted those records: none
// of what it does runs on any other shard.
type DeferredSplitFilter struct {
	MinHash    uint32
	MaxHash    uint32
	ParentTerm int64
	Cursor     []byte `json:",omitempty"`
}

// keeps reports whether the child keeps the record at key, whose entry is se:
// the hash of its partition key places it, if it has one, or the hash of its
// key, as the clients route their requests. An internal key, like the key of a
// session, isn't a record, and is kept.
func (f *DeferredSplitFilter) keeps(key string, se *proto.StorageEntry) bool {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return true
	}
	if se.PartitionKey != nil {
		return f.keepsHash(hash.Xxh332(*se.PartitionKey))
	}
	return f.keepsHash(hash.Xxh332(key))
}

func (f *DeferredSplitFilter) keepsHash(h uint32) bool {
	return h >= f.MinHash && h <= f.MaxHash
}

// keepsValue reports whether the child keeps the record at key, whose entry is
// value, which it reads the partition key of without decoding it. An internal
// key isn't a record, and is kept. A record whose entry can't be read is placed
// by its key, as FilterDBForSplit places it.
func (f *DeferredSplitFilter) keepsValue(key string, value []byte) bool {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return true
	}
	partitionKey, found := entryPartitionKey(value)
	if !found {
		return f.keepsHash(hash.Xxh332(key))
	}
	// The hash doesn't keep the string: it can alias the entry
	return f.keepsHash(hash.Xxh332(unsafe.String(unsafe.SliceData(partitionKey), len(partitionKey))))
}

var storageEntryPartitionKeyField = (&proto.StorageEntry{}).ProtoReflect().Descriptor().Fields().
	ByName("partition_key").Number()

// entryPartitionKey returns the partition key of an encoded storage entry,
// without decoding the entry, and whether it has one, which it doesn't when the
// entry can't be read either.
func entryPartitionKey(entry []byte) ([]byte, bool) {
	var partitionKey []byte
	found := false
	for len(entry) > 0 {
		number, wireType, n := protowire.ConsumeTag(entry)
		if n < 0 {
			return nil, false
		}
		entry = entry[n:]
		if number == storageEntryPartitionKeyField && wireType == protowire.BytesType {
			value, n := protowire.ConsumeBytes(entry)
			if n < 0 {
				return nil, false
			}
			// The last one wins, as when decoding the entry
			partitionKey, found = value, true
			entry = entry[n:]
			continue
		}
		if n = protowire.ConsumeFieldValue(number, wireType, entry); n < 0 {
			return nil, false
		}
		entry = entry[n:]
	}
	return partitionKey, found
}

// SetDeferredSplitFilter records the deferred filter of a split child, seeded
// with a snapshot of its parent, and flushes the database, which runs without
// the Pebble WAL: once the child acks the snapshot, the parent doesn't send it
// again. A parent that is itself the child of an earlier split, seeded with a
// filtered snapshot, has the SplitFilter of that split, which this child drops:
// the child gets no entry of the terms it covers.
func (d *db) SetDeferredSplitFilter(filter *DeferredSplitFilter) error {
	batch := d.kv.NewWriteBatch()
	defer batch.Close()
	if err := d.putDeferredSplitFilter(batch, filter); err != nil {
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
	return nil
}

func (d *db) putDeferredSplitFilter(batch kvstore.WriteBatch, filter *DeferredSplitFilter) error {
	value, err := json.Marshal(filter)
	if err != nil {
		return err
	}
	return d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:   deferredSplitFilterKey,
		Value: value,
	}, now(), NoOpCallback, true, nil, nil, nil)
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

// filterEntry returns se, the entry of the record at key that a write of the
// child's own terms found, or nil if the child doesn't keep the record: it then
// drops the record, with its secondary index entries and its session shadow
// key, and without a notification, as for the write the record doesn't exist.
func (d *db) filterEntry(batch kvstore.WriteBatch, key string, se *proto.StorageEntry, filter *DeferredSplitFilter,
	updateOperationCallback UpdateOperationCallback) (*proto.StorageEntry, error) {
	if filter.keeps(key, se) {
		return se, nil
	}
	defer se.ReturnToVTPool()
	if err := updateOperationCallback.OnDeleteWithEntry(batch, nil, key, se, d); err != nil {
		return nil, err
	}
	return nil, batch.Delete(key)
}

// dropFilteredRecord drops the record at key, whose entry is value, that the
// child doesn't keep, with its secondary index entries and its session shadow
// key, as filterEntry does. A record whose entry can't be read is dropped with
// no internal key, as FilterDBForSplit drops it.
func (d *db) dropFilteredRecord(batch kvstore.WriteBatch, key string, value []byte,
	updateOperationCallback UpdateOperationCallback) error {
	se := proto.StorageEntryFromVTPool()
	defer se.ReturnToVTPool()
	if DeserializeMetadata(value, se) == nil {
		if err := updateOperationCallback.OnDeleteWithEntry(batch, nil, key, se, d); err != nil {
			return err
		}
	}
	return batch.Delete(key)
}

// applySplitFilter applies a step of the deferred split filter: it goes through
// the records from the cursor of the filter on, up to the bounds of the step
// (see proto.SplitFilterRequest), and drops the ones the child doesn't keep.
// Every replica applies the step to the same records, and drops the same ones.
// It returns the filter as the step leaves it: with the cursor after the last
// record it went through, or nil once it went through the last one, which
// ends the filter.
func (d *db) applySplitFilter(batch kvstore.WriteBatch, step *proto.SplitFilterRequest,
	updateOperationCallback UpdateOperationCallback) (*DeferredSplitFilter, error) {
	filter := d.deferredSplitFilter.Load()
	if filter == nil {
		// The filter ended already, e.g. with the steps of an earlier leader
		return nil, nil //nolint:nilnil
	}

	maxRecords := max(step.MaxRecords, 1)
	var records uint32
	var size uint64
	next, err := d.kv.Scan(filter.Cursor, func(key string, value []byte) (bool, error) {
		records++
		size += uint64(len(value))
		if !filter.keepsValue(key, value) {
			if err := d.dropFilteredRecord(batch, key, value, updateOperationCallback); err != nil {
				return false, err
			}
		}
		return records < maxRecords && (step.MaxBytes == 0 || size < uint64(step.MaxBytes)), nil
	})
	if err != nil {
		return nil, err
	}
	if next == nil {
		return nil, batch.Delete(deferredSplitFilterKey)
	}
	updated := *filter
	updated.Cursor = next
	return &updated, d.putDeferredSplitFilter(batch, &updated)
}

// KeepsIndexEntry reports whether a split child with the deferred filter keeps
// the entry of a secondary index at indexKey: it keeps the record of the
// entry, and the record still has the entry. The reads of an index skip the
// other entries, until the child deletes them with their records.
func (d *db) KeepsIndexEntry(filter *DeferredSplitFilter, indexKey string) (bool, error) {
	primaryKey, _, err := ParseSecondaryIndexKey(indexKey)
	if err != nil {
		// Not an entry of an index: the reads of the index handle it
		return true, nil
	}
	_, value, closer, err := d.kv.Get(primaryKey, kvstore.ComparisonEqual, kvstore.NoInternalKeys)
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	kept := filter.keepsValue(primaryKey, value)
	se := proto.StorageEntryFromVTPool()
	defer se.ReturnToVTPool()
	if !kept || DeserializeMetadata(value, se) != nil {
		// The entries of a record that can't be read can't be told: the reads
		// of the index handle it, as on any other shard
		return kept, closer.Close()
	}
	if err := closer.Close(); err != nil {
		return false, err
	}
	escapedKey := url.PathEscape(primaryKey)
	for _, si := range se.SecondaryIndexes {
		if secondaryIndexEntryKey(escapedKey, si) == indexKey {
			return true, nil
		}
	}
	return false, nil
}

// getKept is Get on a split child that holds records it doesn't keep: the
// record it returns is the closest to the key of the request, by its
// comparison, among the ones the child keeps.
func (d *db) getKept(getReq *proto.GetRequest, filter *DeferredSplitFilter) (*proto.GetResponse, error) {
	if getReq.ComparisonType == proto.KeyComparisonType_EQUAL {
		return d.getKeptEqual(getReq, filter)
	}

	it, err := d.kv.Seek(getReq.Key, kvstore.ComparisonType(getReq.ComparisonType))
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}
	next := it.Next
	if getReq.ComparisonType == proto.KeyComparisonType_FLOOR || getReq.ComparisonType == proto.KeyComparisonType_LOWER {
		next = it.Prev
	}
	return getKeptClosest(it, getReq, filter, next)
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
// keeps, or an internal key, and reports whether it did.
func (it *keptRecordsIterator) skip(next func() bool) bool {
	for it.KeyValueIterator.Valid() {
		key := it.Key()
		if strings.HasPrefix(key, constant.InternalKeyPrefix) {
			return true
		}
		value, err := it.Value()
		if err != nil {
			it.err = err
			return false
		}
		if it.filter.keepsValue(key, value) {
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
