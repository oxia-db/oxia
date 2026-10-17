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

func secondaryIndexKey(primaryKey string, si *proto.SecondaryIndex) string {
	return secondaryIdxKeyPrefix + "/" + si.IndexName + secondaryIdxNameSeparator + si.SecondaryKey +
		secondaryIdxSeparator + url.PathEscape(primaryKey)
}

func deleteSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string, existingEntry *proto.StorageEntry) error {
	if len(existingEntry.SecondaryIndexes) > 0 {
		for _, si := range existingEntry.SecondaryIndexes {
			if err := batch.Delete(secondaryIndexKey(primaryKey, si)); err != nil {
				return err
			}
		}
	}
	return nil
}

var emptyValue []byte

func writeSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string, secondaryIndexes []*proto.SecondaryIndex) error {
	if len(secondaryIndexes) > 0 {
		for _, si := range secondaryIndexes {
			if err := batch.Put(secondaryIndexKey(primaryKey, si), emptyValue); err != nil {
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

	for _, si := range existing {
		if entries[newSecondaryIndexEntry(si)]&inUpdated == 0 {
			if err := batch.Delete(secondaryIndexKey(primaryKey, si)); err != nil {
				return err
			}
		}
	}
	for _, si := range updated {
		if entries[newSecondaryIndexEntry(si)]&inExisting == 0 {
			if err := batch.Put(secondaryIndexKey(primaryKey, si), emptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexListIterator(req *proto.ListRequest, db database.DB) (kvstore.KeyIterator, error) {
	return newSecondaryIndexIterator(db, req)
}

// secondaryIndexRange is a list or a range scan request through an index.
type secondaryIndexRange interface {
	GetSecondaryIndexName() string
	GetStartInclusive() string
	GetEndExclusive() string
	GetEndInclusive() bool
}

// newSecondaryIndexIterator returns an iterator over the entries of the index of
// a request whose secondary key is in [start, end), followed, if the end is
// inclusive and the start is not after it, by the entries of the end.
func newSecondaryIndexIterator(db database.DB, r secondaryIndexRange) (*secondaryIndexListIterator, error) {
	start, end := r.GetStartInclusive(), r.GetEndExclusive()
	indexPrefix := fmt.Sprintf(secondaryIdxRangePrefixFormat, r.GetSecondaryIndexName(), "")
	// The iterator only visits the entries of the index. With the hierarchical
	// key sorting, they are not contiguous: they are grouped by level, and other
	// internal keys sort between the groups, like the entries of other indexes
	// and the session shadow keys. A range across levels would include them.
	// The split filter is loaded before the iterator, which holds the entries
	// of the time it's created (see database.DB.RangeScan).
	splitFilter := db.DeferredSplitFilter()
	listIt := &secondaryIndexListIterator{}
	readEnd := secondaryIndexRangeBound(indexPrefix, end)
	if r.GetEndInclusive() && db.CompareKeys(start, end) <= 0 {
		// The entries of the end key are the ones that start with the key and
		// the separator, without another separator after it, since the escaped
		// primary key has none. They sort after the end bound, and before the
		// key followed by the byte after the separator, in either key sorting,
		// where the iterator stops instead.
		listIt.db = db
		listIt.end = readEnd
		listIt.endKeyEntries = indexPrefix + end + secondaryIdxSeparator
		readEnd = indexPrefix + end + "\x02"
	}
	it, err := db.ListPrefix(indexPrefix, readEnd)
	if err != nil {
		return nil, err
	}
	// The iterator stops at the end, so keptIndexEntries needs none
	listIt.it = keptIndexEntries(db, splitFilter, it, "")

	listIt.it.SeekGE(secondaryIndexRangeBound(indexPrefix, start))
	listIt.readKey()
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
	it    database.PrefixIterator
	key   string
	valid bool
	// With an inclusive end, the end bound of the range without its end, and
	// the prefix of the entries of the end key, which the iterator goes through
	// after that range, skipping the ones of the secondary keys that extend the
	// end key with the separator
	db            database.DB
	end           string
	endKeyEntries string
}

func (it *secondaryIndexListIterator) Valid() bool {
	return it.valid
}

// readKey reads the key of the entry that the iterator is on, if any, and
// reports whether it is on one. With an inclusive end, it goes from the range
// without its end to the entries of the end key.
func (it *secondaryIndexListIterator) readKey() bool {
	for it.valid = it.it.Valid(); it.valid; it.valid = it.it.Valid() {
		it.key = it.it.Key()
		if it.endKeyEntries == "" || it.db.CompareKeys(it.key, it.end) < 0 {
			return true
		}
		switch {
		case it.db.CompareKeys(it.key, it.endKeyEntries) < 0:
			// With the hierarchical sorting, the entries of a key ending in "//"
			// are one level after the end bound, which can be a whole level of
			// entries before them
			it.it.SeekGE(it.endKeyEntries)
		case !strings.Contains(it.key[len(it.endKeyEntries):], secondaryIdxSeparator):
			// The entries from endKeyEntries to the end of the iterator all start
			// with it, in either key sorting, and only the ones of the end key
			// have no other separator
			return true
		default:
			// The entry of a secondary key that extends the end key
			it.it.Next()
		}
	}
	return false
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
	return it.readKey()
}

func (it *secondaryIndexListIterator) Error() error {
	return it.it.Error()
}

func (it *secondaryIndexListIterator) Close() error {
	return it.it.Close()
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexRangeScanIterator(req *proto.RangeScanRequest, db database.DB) (database.RangeScanIterator, error) {
	listIt, err := newSecondaryIndexIterator(db, req)
	if err != nil {
		return nil, err
	}

	it := &secondaryIndexRangeIterator{listIt: listIt}
	it.readRecord()
	return it, nil
}

// secondaryIndexRangeIterator returns the records of the entries of an index,
// as they were when it was created, like the entries: a record written or
// deleted since comes as it was, with its entry. It skips the entries without
// a record, which the session ends left behind without
// FEATURE_EPHEMERAL_SECONDARY_INDEX_CLEANUP.
type secondaryIndexRangeIterator struct {
	listIt *secondaryIndexListIterator
	// The record of the entry that the iterator is on, or the error of its read
	record *proto.GetResponse
	err    error
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
	it.listIt.Next()
	return it.readRecord()
}

func (it *secondaryIndexRangeIterator) Value() (*proto.GetResponse, error) {
	return it.record, it.err
}

// readRecord reads the record of the entry that the iterator is on, moving to
// the next entry while the record is not found, and reports whether the
// iterator is on an entry.
func (it *secondaryIndexRangeIterator) readRecord() bool {
	for ; it.listIt.Valid(); it.listIt.Next() {
		primaryKey, secondaryKey := it.listIt.keys()
		it.record, it.err = it.listIt.it.Record(primaryKey, true)
		if it.err != nil {
			return true
		}
		if it.record.Status != proto.Status_KEY_NOT_FOUND {
			it.record.Key = &primaryKey
			// The records come in the order of their secondary keys: clients
			// need them to merge the records of several shards in that order
			it.record.SecondaryIndexKey = &secondaryKey
			return true
		}
	}
	return false
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func secondaryIndexGet(req *proto.GetRequest, db database.DB) (*proto.GetResponse, error) {
	indexPrefix := fmt.Sprintf(secondaryIdxRangePrefixFormat, *req.SecondaryIndexName, "")
	// The iterator only visits the entries of the index. With the hierarchical
	// key sorting, they are not contiguous: they are grouped by level, and other
	// internal keys, like the entries of other indexes, sort between the groups.
	// The split filter is loaded before the iterator, which holds the entries
	// of the time it's created (see database.DB.RangeScan).
	splitFilter := db.DeferredSplitFilter()
	it, err := db.KeyPrefixIterator(indexPrefix, "")
	if err != nil {
		return nil, err
	}
	it = keptIndexEntries(db, splitFilter, it, secondaryGetSkipEnd(indexPrefix, req))

	defer func() { _ = it.Close() }()

	primaryKey, secondaryKey, err := doSecondaryGet(db, it, indexPrefix, req)
	if err != nil && !errors.Is(err, database.ErrInvalidSecondaryIndexKey) {
		return nil, err
	}

	if primaryKey == "" {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	}

	// The record as it was when the iterator found its entry
	gr, err := it.Record(primaryKey, req.IncludeValue)

	if gr != nil {
		gr.Key = &primaryKey
		gr.SecondaryIndexKey = &secondaryKey
	}
	return gr, err
}

// doSecondaryGet returns the keys of the entry that req, a get of the index at
// indexPrefix, finds with it, an iterator over the entries of the index.
//
//nolint:revive
func doSecondaryGet(db database.DB, it kvstore.KeyIterator, indexPrefix string, req *proto.GetRequest) (
	primaryKey string, secondaryKey string, err error) {
	// Entries are stored as indexPrefix + secondary key + separator + escaped
	// primary key. With the separator, the search key sorts right before the
	// entries of the requested key in either key sorting. Without it, the search
	// key for "/", or for a key ending in "//", would end in "//", which the
	// hierarchical sorting does not count as a level.
	searchKey := indexPrefix + req.Key + secondaryIdxSeparator

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

// secondaryGetSkipEnd returns the entry of the index at indexPrefix up to which
// a get of req looks for an entry of a record that a split child keeps (see
// keptIndexEntries). An EQUAL or FLOOR get finds an entry of the secondary key
// of the request, or one before, so it stops after the entries of the key: they
// end in the separator, with the escaped primary key, which has no '/', and
// sort before the key followed by the byte after the separator in either key
// sorting. The other gets go as far as they need.
func secondaryGetSkipEnd(indexPrefix string, req *proto.GetRequest) string {
	switch req.ComparisonType {
	case proto.KeyComparisonType_EQUAL, proto.KeyComparisonType_FLOOR:
		return indexPrefix + req.Key + "\x02"
	default:
		return ""
	}
}
