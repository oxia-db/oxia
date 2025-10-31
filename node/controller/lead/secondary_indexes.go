// Copyright 2023-2025 The Oxia Authors
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

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/node/constant"
	"github.com/oxia-db/oxia/node/storage"
	"github.com/oxia-db/oxia/node/storage/kvstore"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/proto"
)

func NewSecondaryIndexListIterator(req *proto.ListRequest, db storage.DB) (kvstore.KeyIterator, error) {
	indexName := *req.SecondaryIndexName
	it, err := db.List(&proto.ListRequest{
		StartInclusive: fmt.Sprintf(constant.SecondaryIdxRangePrefixFormat, indexName, req.StartInclusive),
		EndExclusive:   fmt.Sprintf(constant.SecondaryIdxRangePrefixFormat, indexName, req.EndExclusive),
	})
	if err != nil {
		return nil, err
	}

	return &secondaryIndexListIterator{it: it}, nil
}

type secondaryIndexListIterator struct {
	it kvstore.KeyIterator
}

func (it *secondaryIndexListIterator) Valid() bool {
	return it.it.Valid()
}

func (it *secondaryIndexListIterator) Key() string {
	idxKey := it.it.Key()
	primaryKey, err := constant.SecondaryIndexPrimaryKey(idxKey)
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

func NewSecondaryIndexRangeScanIterator(req *proto.RangeScanRequest, db storage.DB) (storage.RangeScanIterator, error) {
	indexName := *req.SecondaryIndexName
	it, err := db.List(&proto.ListRequest{
		StartInclusive: fmt.Sprintf(constant.SecondaryIdxRangePrefixFormat, indexName, req.StartInclusive),
		EndExclusive:   fmt.Sprintf(constant.SecondaryIdxRangePrefixFormat, indexName, req.EndExclusive),
	})
	if err != nil {
		return nil, err
	}

	return &secondaryIndexRangeIterator{listIt: &secondaryIndexListIterator{it},
		db: db}, nil
}

type secondaryIndexRangeIterator struct {
	listIt *secondaryIndexListIterator
	db     storage.DB
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

func SecondaryIndexGet(req *proto.GetRequest, db storage.DB) (*proto.GetResponse, error) {
	primaryKey, secondaryKey, err := doSecondaryGet(db, req)
	if err != nil && !errors.Is(err, constant.ErrFailedToParseSecondaryKey) {
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
func doSecondaryGet(db storage.DB, req *proto.GetRequest) (primaryKey string, secondaryKey string, err error) {
	indexName := *req.SecondaryIndexName
	searchKey := fmt.Sprintf(constant.SecondaryIdxRangePrefixFormat, indexName, req.Key)
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

	if !it.Valid() && (req.ComparisonType == proto.KeyComparisonType_FLOOR ||
		req.ComparisonType == proto.KeyComparisonType_CEILING) {
		// There might be more keys in the db. Let's try to compare it
		// to the highest one
		it.Prev()
	}

	for it.Valid() {
		itKey := it.Key()
		primaryKey, secondaryKey, err = constant.SecondaryIndexPrimaryAndSecondaryKey(itKey)
		if err != nil && !errors.Is(err, constant.ErrFailedToParseSecondaryKey) {
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
		}
	}

	if !it.Valid() {
		primaryKey = ""
	}

	return primaryKey, secondaryKey, err
}
