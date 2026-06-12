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
	"sort"

	"github.com/oxia-db/oxia/common/proto"
)

// sortedNotificationBatch converts a NotificationBatch into its
// wire-compatible twin (SortedNotificationBatch) with the map entries sorted
// by key. A protobuf map field is encoded identically to a repeated entry
// message, and the generated vtproto marshal emits repeated fields in slice
// order — so the result marshals to deterministic bytes with generated code
// only. Determinism is a correctness requirement: the notification value
// feeds the replicated batch checksum, and every replica — including ones
// running the previous protobuf-go Deterministic marshaler — must produce
// identical bytes (pinned by TestSortedNotificationBatchMatchesStdlib).
func sortedNotificationBatch(nb *proto.NotificationBatch) *proto.SortedNotificationBatch {
	keys := make([]string, 0, len(nb.Notifications))
	for k := range nb.Notifications {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]*proto.NotificationBatchEntry, len(keys))
	for i, k := range keys {
		entries[i] = &proto.NotificationBatchEntry{Key: &k, Value: nb.Notifications[k]}
	}

	return &proto.SortedNotificationBatch{
		Shard:         nb.Shard,
		Offset:        nb.Offset,
		Timestamp:     nb.Timestamp,
		Notifications: entries,
	}
}
