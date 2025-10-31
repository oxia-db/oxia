package db

import (
	"github.com/oxia-db/oxia/node/db/kv"
	"github.com/oxia-db/oxia/proto"
)

type UpdateOperationCallback interface {
	OnPut(batch kv.WriteBatch, notifications *Notifications, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error)
	OnDelete(batch kv.WriteBatch, notifications *Notifications, key string) error
	OnDeleteWithEntry(batch kv.WriteBatch, notifications *Notifications, key string, value *proto.StorageEntry) error
	OnDeleteRange(batch kv.WriteBatch, notifications *Notifications, keyStartInclusive string, keyEndExclusive string) error
}

type wrapperUpdateCallback struct{}

func (wrapperUpdateCallback) OnDeleteWithEntry(batch kv.WriteBatch, notifications *Notifications, key string, value *proto.StorageEntry) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteWithEntry(batch, notifications, key, value); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteWithEntry(batch, notifications, key, value)
}

func (wrapperUpdateCallback) OnPut(batch kv.WriteBatch, notifications *Notifications, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error) {
	// First update the session
	status, err := sessionManagerUpdateOperationCallback.OnPut(batch, notifications, req, se)
	if err != nil || status != proto.Status_OK {
		return status, err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnPut(batch, notifications, req, se)
}

func (wrapperUpdateCallback) OnDelete(batch kv.WriteBatch, notifications *Notifications, key string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDelete(batch, notifications, key); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDelete(batch, notifications, key)
}

func (wrapperUpdateCallback) OnDeleteRange(batch kv.WriteBatch, notifications *Notifications, keyStartInclusive string, keyEndExclusive string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteRange(batch, notifications, keyStartInclusive, keyEndExclusive); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteRange(batch, notifications, keyStartInclusive, keyEndExclusive)
}
