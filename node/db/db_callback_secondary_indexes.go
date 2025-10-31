package db

import (
	. "github.com/oxia-db/oxia/node/constant"
	. "github.com/oxia-db/oxia/node/db/kv"
	"github.com/oxia-db/oxia/proto"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

var WrapperUpdateOperationCallback UpdateOperationCallback = &wrapperUpdateCallback{}

type secondaryIndexesUpdateCallbackS struct{}

var secondaryIndexesUpdateCallback UpdateOperationCallback = &secondaryIndexesUpdateCallbackS{}

func (secondaryIndexesUpdateCallbackS) OnPut(batch WriteBatch, _ *Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	if existingEntry != nil {
		// TODO: We might want to check if there are indexes that did not change
		// between the existing and the new record.
		if err := deleteSecondaryIndexes(batch, request.Key, existingEntry); err != nil {
			return proto.Status_KEY_NOT_FOUND, err
		}
	}

	return proto.Status_OK, writeSecondaryIndexes(batch, request.Key, request.SecondaryIndexes)
}

func (secondaryIndexesUpdateCallbackS) OnDelete(batch WriteBatch, _ *Notifications, key string) error {
	se, err := GetStorageEntry(batch, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil
		}
		return err
	}
	defer se.ReturnToVTPool()
	return deleteSecondaryIndexes(batch, key, se)
}

func (secondaryIndexesUpdateCallbackS) OnDeleteWithEntry(batch WriteBatch, _ *Notifications, key string, value *proto.StorageEntry) error {
	return deleteSecondaryIndexes(batch, key, value)
}

func (secondaryIndexesUpdateCallbackS) OnDeleteRange(batch WriteBatch, _ *Notifications, keyStartInclusive string, keyEndExclusive string) error {
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

func deleteSecondaryIndexes(batch WriteBatch, primaryKey string, existingEntry *proto.StorageEntry) error {
	if len(existingEntry.SecondaryIndexes) > 0 {
		for _, si := range existingEntry.SecondaryIndexes {
			if err := batch.Delete(SecondaryIndexKey(primaryKey, si)); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeSecondaryIndexes(batch WriteBatch, primaryKey string, secondaryIndexes []*proto.SecondaryIndex) error {
	if len(secondaryIndexes) > 0 {
		for _, si := range secondaryIndexes {
			if err := batch.Put(SecondaryIndexKey(primaryKey, si), EmptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}
