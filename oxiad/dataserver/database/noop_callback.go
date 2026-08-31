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
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

type noopCallback struct{}

func (*noopCallback) ValidatePut(*proto.PutRequest, feature.Checker) proto.Status {
	return proto.Status_OK
}

func (*noopCallback) OnDeleteWithEntry(kvstore.WriteBatch, *Notifications, string, *proto.StorageEntry) error {
	return nil
}

func (*noopCallback) OnPut(_ kvstore.WriteBatch, _ *Notifications, _ *proto.PutRequest, _ *proto.StorageEntry) (proto.Status, error) {
	return proto.Status_OK, nil
}

func (*noopCallback) OnDelete(_ kvstore.WriteBatch, _ *Notifications, _ string) error {
	return nil
}

func (*noopCallback) OnDeleteRange(_ kvstore.WriteBatch, _ *Notifications, _ string, _ string) error {
	return nil
}

var NoOpCallback UpdateOperationCallback = &noopCallback{}
