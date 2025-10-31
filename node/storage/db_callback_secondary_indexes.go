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

package storage

import (
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/node/constant"

	"github.com/oxia-db/oxia/node/storage/kvstore"
	"github.com/oxia-db/oxia/proto"
)

var WrapperUpdateOperationCallback UpdateOperationCallback = &wrapperUpdateCallback{}

type secondaryIndexesUpdateCallbackS struct{}

var secondaryIndexesUpdateCallback UpdateOperationCallback = &secondaryIndexesUpdateCallbackS{}

func (secondaryIndexesUpdateCallbackS) OnPut(batch kvstore.WriteBatch, _ *Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	if existingEntry != nil {
		// TODO: We might want to check if there are indexes that did not change
		// between the existing and the new record.
		if err := deleteSecondaryIndexes(batch, request.Key, existingEntry); err != nil {
			return proto.Status_KEY_NOT_FOUND, err
		}
	}

	return proto.Status_OK, writeSecondaryIndexes(batch, request.Key, request.SecondaryIndexes)
}

func (secondaryIndexesUpdateCallbackS) OnDelete(batch kvstore.WriteBatch, _ *Notifications, key string) error {
	se, err := GetStorageEntry(batch, key)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	defer se.ReturnToVTPool()
	return deleteSecondaryIndexes(batch, key, se)
}

func (secondaryIndexesUpdateCallbackS) OnDeleteWithEntry(batch kvstore.WriteBatch, _ *Notifications, key string, value *proto.StorageEntry) error {
	return deleteSecondaryIndexes(batch, key, value)
}

func (secondaryIndexesUpdateCallbackS) OnDeleteRange(batch kvstore.WriteBatch, _ *Notifications, keyStartInclusive string, keyEndExclusive string) error {
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

		err = Deserialize(value, se)
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

func deleteSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string, existingEntry *proto.StorageEntry) error {
	if len(existingEntry.SecondaryIndexes) > 0 {
		for _, si := range existingEntry.SecondaryIndexes {
			if err := batch.Delete(constant.SecondaryIndexKey(primaryKey, si)); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeSecondaryIndexes(batch kvstore.WriteBatch, primaryKey string, secondaryIndexes []*proto.SecondaryIndex) error {
	if len(secondaryIndexes) > 0 {
		for _, si := range secondaryIndexes {
			if err := batch.Put(constant.SecondaryIndexKey(primaryKey, si), constant.EmptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}
