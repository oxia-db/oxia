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

package constant

import (
	"time"

	"github.com/oxia-db/oxia/node/wal"
	"github.com/oxia-db/oxia/proto"
)

const (
	MetadataTerm      = "term"
	MetadataNamespace = "namespace"
	MetadataShardId   = "shard-id"
	DefaultNamespace  = "default"

	DefaultPublicPort   = 6648
	DefaultInternalPort = 6649
	DefaultAdminPort    = 6651
	DefaultMetricsPort  = 8080
)

var InvalidEntryId = &proto.EntryId{
	Term:   wal.InvalidTerm,
	Offset: wal.InvalidOffset,
}
