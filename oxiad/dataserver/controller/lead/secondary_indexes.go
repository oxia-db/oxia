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
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/proto"
)

const secondaryIdxKeyPrefix = constant.InternalKeyPrefix + "idx"

type wrapperUpdateCallback struct{}

func (wrapperUpdateCallback) ValidatePut(req *proto.PutRequest, features feature.Checker) proto.Status {
	if status := sessionManagerUpdateOperationCallback.ValidatePut(req, features); status != proto.Status_OK {
		return status
	}

	return secondaryIndexesUpdateCallback.ValidatePut(req, features)
}

func (wrapperUpdateCallback) OnDeleteWithEntry(batch kvstore.WriteBatch, notifications *database.Notifications, key string, value *proto.StorageEntry, features feature.Checker) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteWithEntry(batch, notifications, key, value, features); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteWithEntry(batch, notifications, key, value, features)
}

func (wrapperUpdateCallback) OnPut(batch kvstore.WriteBatch, notifications *database.Notifications, req *proto.PutRequest, se *proto.StorageEntry, features feature.Checker) (proto.Status, error) {
	// First update the session
	status, err := sessionManagerUpdateOperationCallback.OnPut(batch, notifications, req, se, features)
	if err != nil || status != proto.Status_OK {
		return status, err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnPut(batch, notifications, req, se, features)
}

var WrapperUpdateOperationCallback database.UpdateOperationCallback = &wrapperUpdateCallback{}

type secondaryIndexesUpdateCallbackS struct{}

var secondaryIndexesUpdateCallback database.UpdateOperationCallback = &secondaryIndexesUpdateCallbackS{}

func (secondaryIndexesUpdateCallbackS) ValidatePut(request *proto.PutRequest, features feature.Checker) proto.Status {
	if !features.IsFeatureEnabled(proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION) {
		return proto.Status_OK
	}

	for _, secondaryIndex := range request.SecondaryIndexes {
		if strings.IndexByte(secondaryIndex.GetIndexName(), '/') >= 0 {
			return proto.Status_INVALID_ARGUMENT
		}
	}
	return proto.Status_OK
}

func (secondaryIndexesUpdateCallbackS) OnPut(batch kvstore.WriteBatch, _ *database.Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry, features feature.Checker) (proto.Status, error) {
	if existingEntry != nil {
		if features.IsFeatureEnabled(proto.Feature_FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED) {
			return proto.Status_OK, updateSecondaryIndexes(batch, request.Key, existingEntry.SecondaryIndexes,
				request.SecondaryIndexes)
		}
		// Without the feature, the entries are all deleted and written again,
		// as by the servers that don't support it: the write batch feeds the DB
		// checksum, which must be the same on every replica
		if err := deleteSecondaryIndexes(batch, request.Key, existingEntry); err != nil {
			return proto.Status_KEY_NOT_FOUND, err
		}
	}

	return proto.Status_OK, writeSecondaryIndexes(batch, request.Key, request.SecondaryIndexes)
}

func (secondaryIndexesUpdateCallbackS) OnDeleteWithEntry(batch kvstore.WriteBatch, _ *database.Notifications, key string, value *proto.StorageEntry, _ feature.Checker) error {
	return deleteSecondaryIndexes(batch, key, value)
}

const secondaryIdxSeparator = "\x01"
const secondaryIdxRangePrefixFormat = secondaryIdxKeyPrefix + "/%s/%s"

// secondaryIdxNameSeparator separates the index name and the secondary key in
// the key of an index entry.
const secondaryIdxNameSeparator = "/"

// secondaryIndexKey returns the key of the entry of a secondary index of the
// record whose key, escaped with url.PathEscape, is escapedPrimaryKey.
func secondaryIndexKey(escapedPrimaryKey string, si *proto.SecondaryIndex) string {
	return secondaryIdxKeyPrefix + "/" + si.IndexName + secondaryIdxNameSeparator + si.SecondaryKey +
		secondaryIdxSeparator + escapedPrimaryKey
}

func deleteSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string, existingEntry *proto.StorageEntry) error {
	if len(existingEntry.SecondaryIndexes) > 0 {
		escapedPrimaryKey := url.PathEscape(primaryKey)
		for _, si := range existingEntry.SecondaryIndexes {
			if err := batch.Delete(secondaryIndexKey(escapedPrimaryKey, si)); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteKeySecondaryIndexes deletes the secondary indexes of the record at key,
// which it reads from the record's entry.
func deleteKeySecondaryIndexes(batch kvstore.WriteBatch, key string) error {
	se, err := database.GetStorageEntryMetadata(batch, key)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	defer se.ReturnToVTPool()
	return deleteSecondaryIndexes(batch, key, se)
}

var emptyValue []byte

func writeSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string, secondaryIndexes []*proto.SecondaryIndex) error {
	if len(secondaryIndexes) > 0 {
		escapedPrimaryKey := url.PathEscape(primaryKey)
		for _, si := range secondaryIndexes {
			if err := batch.Put(secondaryIndexKey(escapedPrimaryKey, si), emptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// secondaryIndexEntry identifies the entry that a secondary index gives a
// record: two indexes of the record give it the same entry when they have the
// same secondaryIndexEntry.
type secondaryIndexEntry struct {
	indexName    string
	secondaryKey string
}

func newSecondaryIndexEntry(si *proto.SecondaryIndex) secondaryIndexEntry {
	// Index names could have a '/' before
	// FEATURE_SECONDARY_INDEX_NAME_VALIDATION. The entry key of such an index
	// is the one of the index named by the part of the name before the first
	// '/', with a secondary key that starts with the rest of the name
	if indexName, rest, found := strings.Cut(si.IndexName, secondaryIdxNameSeparator); found {
		return secondaryIndexEntry{indexName: indexName, secondaryKey: rest + secondaryIdxNameSeparator + si.SecondaryKey}
	}
	return secondaryIndexEntry{indexName: si.IndexName, secondaryKey: si.SecondaryKey}
}

// updateSecondaryIndexes updates the index entries of a record that a put
// overwrites, from its existing indexes to the updated ones. The entries of the
// indexes that the record keeps are already there: it deletes the entries of
// the indexes it drops, then writes the ones of the indexes it adds.
func updateSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string,
	existing, updated []*proto.SecondaryIndex) error {
	if slices.EqualFunc(existing, updated, func(a, b *proto.SecondaryIndex) bool {
		return a.IndexName == b.IndexName && a.SecondaryKey == b.SecondaryKey
	}) {
		// The usual overwrite: the record keeps all its indexes
		return nil
	}

	// A map, rather than a scan of the other indexes for each one, keeps the
	// work linear in the number of indexes of the record
	const (
		inExisting = 1 << iota
		inUpdated
	)
	entries := make(map[secondaryIndexEntry]uint8)
	for _, si := range existing {
		entries[newSecondaryIndexEntry(si)] |= inExisting
	}
	for _, si := range updated {
		entries[newSecondaryIndexEntry(si)] |= inUpdated
	}

	escapedPrimaryKey := url.PathEscape(primaryKey)
	for _, si := range existing {
		if entries[newSecondaryIndexEntry(si)]&inUpdated == 0 {
			if err := batch.Delete(secondaryIndexKey(escapedPrimaryKey, si)); err != nil {
				return err
			}
		}
	}
	for _, si := range updated {
		if entries[newSecondaryIndexEntry(si)]&inExisting == 0 {
			if err := batch.Put(secondaryIndexKey(escapedPrimaryKey, si), emptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexListIterator(req *proto.ListRequest, db database.DB) (kvstore.KeyIterator, error) {
	return newSecondaryIndexIterator(db, *req.SecondaryIndexName, req.StartInclusive, req.EndExclusive)
}

// newSecondaryIndexIterator returns an iterator over the entries of an index
// whose secondary key is in [start, end).
func newSecondaryIndexIterator(db database.DB, indexName, start, end string) (*secondaryIndexListIterator, error) {
	indexPrefix := fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, "")
	// The iterator only visits the entries of the index. With the hierarchical
	// key sorting, they are not contiguous: they are grouped by level, and other
	// internal keys sort between the groups, like the entries of other indexes
	// and the session shadow keys. A range across levels would include them.
	it, err := db.ListPrefix(indexPrefix)
	if err != nil {
		return nil, err
	}

	it.SeekGE(secondaryIndexRangeBound(indexPrefix, start))
	listIt := &secondaryIndexListIterator{it: it, db: db, end: secondaryIndexRangeBound(indexPrefix, end)}
	listIt.stopAtEnd()
	return listIt, nil
}

// secondaryIndexRangeBound returns the key that bounds a range through an
// index at a secondary key: the prefix of the index followed by the key. It
// sorts among the entries of the index as the key sorts among the primary
// keys, in either key sorting. Unlike the search key of a get, it does not end
// in the separator, so that a key ending in "//" keeps the level that the
// hierarchical sorting gives it, as in the range of the children of a key,
// like ["/a/", "/a//").
//
// The entries of a secondary key ending in "//" are the exception: they end in
// the separator and the escaped primary key, so the hierarchical sorting puts
// them one level after the key, and a range can miss them or return them out
// of order.
func secondaryIndexRangeBound(indexPrefix, key string) string {
	if key == "/" {
		// The prefix ends in '/', so the bound would end in "//", unlike the
		// key, and sort before all the secondary keys with a '/' with the
		// hierarchical sorting. The zero byte keeps it in the level of "/": it
		// makes the smallest string after the bound, so no entry sorts between
		// the two with the natural sorting.
		return indexPrefix + key + "\x00"
	}
	return indexPrefix + key
}

type secondaryIndexListIterator struct {
	it    kvstore.KeyIterator
	db    database.DB
	end   string
	key   string
	valid bool
}

func (it *secondaryIndexListIterator) Valid() bool {
	return it.valid
}

// stopAtEnd ends the iteration on the first entry that is not below the end.
// The entries come in the shard's key order, so the following ones are not
// either.
func (it *secondaryIndexListIterator) stopAtEnd() bool {
	it.valid = it.it.Valid()
	if it.valid {
		it.key = it.it.Key()
		it.valid = it.db.CompareKeys(it.key, it.end) < 0
	}
	return it.valid
}

func (it *secondaryIndexListIterator) Key() string {
	primaryKey, _ := it.keys()
	return primaryKey
}

// keys returns the primary key and the secondary key of the index entry.
func (it *secondaryIndexListIterator) keys() (primaryKey string, secondaryKey string) {
	primaryKey, secondaryKey, err := database.ParseSecondaryIndexKey(it.key)
	if err != nil {
		// This should never happen since we control the key format
		panic(errors.Wrap(err, "Failed to parse secondary index key"))
	}

	return primaryKey, secondaryKey
}

func (*secondaryIndexListIterator) Prev() bool {
	panic("not supported")
}

func (*secondaryIndexListIterator) SeekGE(string) bool {
	panic("not supported")
}

func (*secondaryIndexListIterator) SeekLT(string) bool {
	panic("not supported")
}

func (it *secondaryIndexListIterator) Next() bool {
	it.it.Next()
	return it.stopAtEnd()
}

func (it *secondaryIndexListIterator) Error() error {
	return it.it.Error()
}

func (it *secondaryIndexListIterator) Close() error {
	return it.it.Close()
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexRangeScanIterator(req *proto.RangeScanRequest, db database.DB) (database.RangeScanIterator, error) {
	listIt, err := newSecondaryIndexIterator(db, *req.SecondaryIndexName, req.StartInclusive, req.EndExclusive)
	if err != nil {
		return nil, err
	}

	return &secondaryIndexRangeIterator{listIt: listIt, db: db}, nil
}

type secondaryIndexRangeIterator struct {
	listIt *secondaryIndexListIterator
	db     database.DB
}

func (it *secondaryIndexRangeIterator) Close() error {
	return it.listIt.Close()
}

func (it *secondaryIndexRangeIterator) Valid() bool {
	return it.listIt.Valid()
}

func (it *secondaryIndexRangeIterator) Key() string {
	return it.listIt.Key()
}

func (it *secondaryIndexRangeIterator) Next() bool {
	return it.listIt.Next()
}

func (it *secondaryIndexRangeIterator) Value() (*proto.GetResponse, error) {
	primaryKey, secondaryKey := it.listIt.keys()
	gr, err := it.db.Get(&proto.GetRequest{
		Key:            primaryKey,
		IncludeValue:   true,
		ComparisonType: proto.KeyComparisonType_EQUAL,
	})

	if gr != nil {
		gr.Key = &primaryKey
		// The records come in the order of their secondary keys: clients need
		// them to merge the records of several shards in that order
		gr.SecondaryIndexKey = &secondaryKey
	}

	return gr, err
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func secondaryIndexGet(req *proto.GetRequest, db database.DB) (*proto.GetResponse, error) {
	primaryKey, secondaryKey, err := doSecondaryGet(db, req)
	if err != nil && !errors.Is(err, database.ErrInvalidSecondaryIndexKey) {
		return nil, err
	}

	if primaryKey == "" {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	}

	gr, err := db.Get(&proto.GetRequest{
		Key:            primaryKey,
		IncludeValue:   req.IncludeValue,
		ComparisonType: proto.KeyComparisonType_EQUAL,
	})

	if gr != nil {
		gr.Key = &primaryKey
		gr.SecondaryIndexKey = &secondaryKey
	}
	return gr, err
}

//nolint:revive
func doSecondaryGet(db database.DB, req *proto.GetRequest) (primaryKey string, secondaryKey string, err error) {
	indexName := *req.SecondaryIndexName
	indexPrefix := fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, "")
	// Entries are stored as indexPrefix + secondary key + separator + escaped
	// primary key. With the separator, the search key sorts right before the
	// entries of the requested key in either key sorting. Without it, the search
	// key for "/", or for a key ending in "//", would end in "//", which the
	// hierarchical sorting does not count as a level.
	searchKey := indexPrefix + req.Key + secondaryIdxSeparator
	// The iterator only visits the entries of the index. With the hierarchical
	// key sorting, they are not contiguous: they are grouped by level, and other
	// internal keys, like the entries of other indexes, sort between the groups.
	it, err := db.KeyPrefixIterator(indexPrefix)
	if err != nil {
		return "", "", err
	}

	defer func() { _ = it.Close() }()

	if req.ComparisonType == proto.KeyComparisonType_LOWER {
		it.SeekLT(searchKey)
	} else {
		// For all the other cases, we set the iterator on >=
		it.SeekGE(searchKey)

		// A failed read is left to the check after the walk: seeking again
		// would clear the error
		if req.ComparisonType == proto.KeyComparisonType_FLOOR && it.Error() == nil && !it.Valid() {
			// There is no entry of this index at or after the search key: the
			// floor candidate, if any, is the last index entry before it.
			it.SeekLT(searchKey)
		}
	}

	for it.Valid() {
		itKey := it.Key()
		primaryKey, secondaryKey, err = database.ParseSecondaryIndexKey(itKey)
		if err != nil && !errors.Is(err, database.ErrInvalidSecondaryIndexKey) {
			return "", "", err
		}

		// Compare in the order the iterator walks the entries, the shard's key
		// order, with the entry cut down to the form of the search key
		cmp := db.CompareKeys(searchKey, indexPrefix+secondaryKey+secondaryIdxSeparator)

		switch req.ComparisonType {
		case proto.KeyComparisonType_EQUAL:
			if cmp != 0 {
				primaryKey = ""
			}
			return primaryKey, secondaryKey, err

		case proto.KeyComparisonType_FLOOR:
			if primaryKey == "" || cmp < 0 {
				it.Prev()
			} else {
				return primaryKey, secondaryKey, err
			}

		case proto.KeyComparisonType_LOWER:
			if cmp <= 0 {
				it.Prev()
			} else {
				return primaryKey, secondaryKey, err
			}

		case proto.KeyComparisonType_CEILING:
			if cmp > 0 {
				// The key is already over the max
				primaryKey = ""
			}
			return primaryKey, secondaryKey, err

		case proto.KeyComparisonType_HIGHER:
			if cmp >= 0 {
				it.Next()
			} else {
				return primaryKey, secondaryKey, err
			}

		default:
			return "", "", errors.Errorf("unsupported comparison type: %v", req.ComparisonType)
		}
	}

	// The walk also stops when a read fails
	if err = it.Error(); err != nil {
		return "", "", errors.Wrap(err, "failed to read the secondary index")
	}

	// The walk ran out of entries of the requested index without finding a match
	return "", "", nil
}
