// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/streamnative/oxia/common/compare"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
)

const secondaryIdxKeyPrefix = common.InternalKeyPrefix + "idx"

type wrapperUpdateCallback struct{}

func (wrapperUpdateCallback) OnDeleteWithEntry(batch kv.WriteBatch, key string, value *proto.StorageEntry) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteWithEntry(batch, key, value); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteWithEntry(batch, key, value)
}

func (wrapperUpdateCallback) OnPut(batch kv.WriteBatch, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error) {
	// First update the session
	status, err := sessionManagerUpdateOperationCallback.OnPut(batch, req, se)
	if err != nil || status != proto.Status_OK {
		return status, err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnPut(batch, req, se)
}

func (wrapperUpdateCallback) OnDelete(batch kv.WriteBatch, key string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDelete(batch, key); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDelete(batch, key)
}

func (wrapperUpdateCallback) OnDeleteRange(batch kv.WriteBatch, keyStartInclusive string, keyEndExclusive string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteRange(batch, keyStartInclusive, keyEndExclusive); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteRange(batch, keyStartInclusive, keyEndExclusive)
}

var WrapperUpdateOperationCallback kv.UpdateOperationCallback = &wrapperUpdateCallback{}

type secondaryIndexesUpdateCallbackS struct{}

var secondaryIndexesUpdateCallback kv.UpdateOperationCallback = &secondaryIndexesUpdateCallbackS{}

func (secondaryIndexesUpdateCallbackS) OnPut(batch kv.WriteBatch, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	if existingEntry != nil {
		// TODO: We might want to check if there are indexes that did not change
		// between the existing and the new record.
		if err := deleteSecondaryIndexes(batch, request.Key, existingEntry); err != nil {
			return proto.Status_KEY_NOT_FOUND, err
		}
	}

	return proto.Status_OK, writeSecondaryIndexes(batch, request.Key, request.SecondaryIndexes)
}

func (secondaryIndexesUpdateCallbackS) OnDelete(batch kv.WriteBatch, key string) error {
	se, err := kv.GetStorageEntry(batch, key)
	defer se.ReturnToVTPool()

	if errors.Is(err, kv.ErrKeyNotFound) {
		return nil
	}
	if err == nil {
		err = deleteSecondaryIndexes(batch, key, se)
	}
	return err
}

func (secondaryIndexesUpdateCallbackS) OnDeleteWithEntry(batch kv.WriteBatch, key string, value *proto.StorageEntry) error {
	return deleteSecondaryIndexes(batch, key, value)
}

func (secondaryIndexesUpdateCallbackS) OnDeleteRange(batch kv.WriteBatch, keyStartInclusive string, keyEndExclusive string) error {
	it, err := batch.RangeScan(keyStartInclusive, keyEndExclusive)
	if err != nil {
		return err
	}

	for ; it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
		se := proto.StorageEntryFromVTPool()

		err = kv.Deserialize(value, se)
		if err == nil {
			err = deleteSecondaryIndexes(batch, it.Key(), se)
		}

		se.ReturnToVTPool()

		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
	}

	if err := it.Close(); err != nil {
		return errors.Wrap(err, "oxia db: failed to delete range")
	}

	return err
}

const secondaryIdxSeparator = "\x01"
const secondaryIdxRangePrefixFormat = secondaryIdxKeyPrefix + "/%s/%s"
const secondaryIdxFormat = secondaryIdxRangePrefixFormat + secondaryIdxSeparator + "%s"

const regex = "^" + secondaryIdxKeyPrefix + "/[^/]+/([^" + secondaryIdxSeparator + "]+)" + secondaryIdxSeparator + "(.+)$"

var secondaryIdxFormatRegex = regexp.MustCompile(regex)

var errFailedToParseSecondaryKey = errors.New("oxia db: failed to parse secondary index key")

func secondaryIndexKey(primaryKey string, si *proto.SecondaryIndex) string {
	return fmt.Sprintf(secondaryIdxFormat, si.IndexName, si.SecondaryKey, url.PathEscape(primaryKey))
}

func secondaryIndexPrimaryKey(completeKey string) (string, error) {
	matches := secondaryIdxFormatRegex.FindStringSubmatch(completeKey)
	if len(matches) != 3 {
		return "", errFailedToParseSecondaryKey
	}

	return url.PathUnescape(matches[2])
}

func secondaryIndexPrimaryAndSecondaryKey(completeKey string) (primaryKey string, secondaryKey string, err error) {
	matches := secondaryIdxFormatRegex.FindStringSubmatch(completeKey)
	if len(matches) != 3 {
		return "", "", errFailedToParseSecondaryKey
	}

	secondaryKey = matches[1]
	primaryKey, err = url.PathUnescape(matches[2])
	return primaryKey, secondaryKey, err
}

func deleteSecondaryIndexes(batch kv.WriteBatch, primaryKey string, existingEntry *proto.StorageEntry) error {
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

func writeSecondaryIndexes(batch kv.WriteBatch, primaryKey string, secondaryIndexes []*proto.SecondaryIndex) error {
	if len(secondaryIndexes) > 0 {
		for _, si := range secondaryIndexes {
			if err := batch.Put(secondaryIndexKey(primaryKey, si), emptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexListIterator(req *proto.ListRequest, db kv.DB) (kv.KeyIterator, error) {
	indexName := *req.SecondaryIndexName
	it, err := db.List(&proto.ListRequest{
		StartInclusive: fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.StartInclusive),
		EndExclusive:   fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.EndExclusive),
	})
	if err != nil {
		return nil, err
	}

	return &secondaryIndexListIterator{it: it}, nil
}

type secondaryIndexListIterator struct {
	it kv.KeyIterator
}

func (it *secondaryIndexListIterator) Valid() bool {
	return it.it.Valid()
}

func (it *secondaryIndexListIterator) Key() string {
	idxKey := it.it.Key()
	primaryKey, err := secondaryIndexPrimaryKey(idxKey)
	if err != nil {
		// This should never happen since we control the key format
		panic(errors.Wrap(err, "Failed to parse secondary index key"))
	}

	return primaryKey
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
	return it.it.Next()
}

func (it *secondaryIndexListIterator) Close() error {
	return it.it.Close()
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexRangeScanIterator(req *proto.RangeScanRequest, db kv.DB) (kv.RangeScanIterator, error) {
	indexName := *req.SecondaryIndexName
	it, err := db.List(&proto.ListRequest{
		StartInclusive: fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.StartInclusive),
		EndExclusive:   fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.EndExclusive),
	})
	if err != nil {
		return nil, err
	}

	return &secondaryIndexRangeIterator{listIt: &secondaryIndexListIterator{it},
		db: db}, nil
}

type secondaryIndexRangeIterator struct {
	listIt *secondaryIndexListIterator
	db     kv.DB
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
	primaryKey := it.Key()
	gr, err := it.db.Get(&proto.GetRequest{
		Key:            primaryKey,
		IncludeValue:   true,
		ComparisonType: proto.KeyComparisonType_EQUAL,
	})

	if gr != nil {
		gr.Key = &primaryKey
	}

	return gr, err
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func secondaryIndexGet(req *proto.GetRequest, db kv.DB) (*proto.GetResponse, error) {
	primaryKey, secondaryKey, err := doSecondaryGet(db, req)
	if err != nil && !errors.Is(err, errFailedToParseSecondaryKey) {
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
func doSecondaryGet(db kv.DB, req *proto.GetRequest) (primaryKey string, secondaryKey string, err error) {
	indexName := *req.SecondaryIndexName
	searchKey := fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.Key)

	it, err := db.KeyIterator()
	if err != nil {
		return "", "", err
	}

	defer func() { _ = it.Close() }()

	if req.ComparisonType == proto.KeyComparisonType_LOWER {
		it.SeekLT(searchKey)
	} else {
		// For all the other cases, we set the iterator on >=
		it.SeekGE(searchKey)
	}

	for it.Valid() {
		itKey := it.Key()
		primaryKey, secondaryKey, err = secondaryIndexPrimaryAndSecondaryKey(itKey)
		if err != nil && !errors.Is(err, errFailedToParseSecondaryKey) {
			return "", "", err
		}

		cmp := compare.CompareWithSlash([]byte(req.Key), []byte(secondaryKey))

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
			return primaryKey, secondaryKey, err

		case proto.KeyComparisonType_HIGHER:
			if cmp >= 0 {
				it.Next()
			} else {
				return primaryKey, secondaryKey, err
			}
		}
	}

	return primaryKey, secondaryKey, err
}
