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
	"github.com/oxia-db/oxia/node/storage/kvstore"
	"github.com/oxia-db/oxia/proto"
)

type UpdateOperationCallback interface {
	OnPut(batch kvstore.WriteBatch, notifications *Notifications, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error)
	OnDelete(batch kvstore.WriteBatch, notifications *Notifications, key string) error
	OnDeleteWithEntry(batch kvstore.WriteBatch, notifications *Notifications, key string, value *proto.StorageEntry) error
	OnDeleteRange(batch kvstore.WriteBatch, notifications *Notifications, keyStartInclusive string, keyEndExclusive string) error
}

type wrapperUpdateCallback struct{}

func (wrapperUpdateCallback) OnDeleteWithEntry(batch kvstore.WriteBatch, notifications *Notifications, key string, value *proto.StorageEntry) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteWithEntry(batch, notifications, key, value); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteWithEntry(batch, notifications, key, value)
}

func (wrapperUpdateCallback) OnPut(batch kvstore.WriteBatch, notifications *Notifications, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error) {
	// First update the session
	status, err := sessionManagerUpdateOperationCallback.OnPut(batch, notifications, req, se)
	if err != nil || status != proto.Status_OK {
		return status, err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnPut(batch, notifications, req, se)
}

func (wrapperUpdateCallback) OnDelete(batch kvstore.WriteBatch, notifications *Notifications, key string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDelete(batch, notifications, key); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDelete(batch, notifications, key)
}

func (wrapperUpdateCallback) OnDeleteRange(batch kvstore.WriteBatch, notifications *Notifications, keyStartInclusive string, keyEndExclusive string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteRange(batch, notifications, keyStartInclusive, keyEndExclusive); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteRange(batch, notifications, keyStartInclusive, keyEndExclusive)
}
