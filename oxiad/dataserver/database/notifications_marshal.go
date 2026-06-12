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
	"encoding/binary"
	"sort"

	"github.com/planetscale/vtprotobuf/protohelpers"

	"github.com/oxia-db/oxia/common/proto"
)

// deterministicNotificationBatch marshals a NotificationBatch with the
// notifications map emitted in ascending key order. The bytes feed the
// replicated batch checksum, so they must be identical on every replica —
// and identical to the protobuf-go Deterministic marshaler this replaces
// (pinned by TestDeterministicNotificationBatchMatchesStdlib, which also
// fails if the message definition gains fields this copy does not emit).
// The generated MarshalToSizedBufferVT iterates the map in Go's random
// order; this is the same emission with the iteration sorted. unknownFields
// are not emitted: the message is always locally built, never decoded.
type deterministicNotificationBatch struct {
	*proto.NotificationBatch
}

// Protobuf wire tags (field number << 3 | wire type) emitted below, matching
// the generated NotificationBatch marshal code
const (
	wireTagShard            = 0x8  // field 1, varint
	wireTagOffset           = 0x10 // field 2, varint
	wireTagTimestamp        = 0x19 // field 3, fixed64
	wireTagNotificationsMap = 0x22 // field 4, length-delimited map entry
	wireTagMapKey           = 0xa  // map-entry field 1, length-delimited
	wireTagMapValue         = 0x12 // map-entry field 2, length-delimited
)

func (d deterministicNotificationBatch) MarshalToSizedBufferVT(dAtA []byte) (int, error) {
	m := d.NotificationBatch
	if m == nil {
		return 0, nil
	}
	i := len(dAtA)
	if len(m.Notifications) > 0 {
		keys := make([]string, 0, len(m.Notifications))
		for k := range m.Notifications {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		// Backward fill: descending iteration yields ascending key order on
		// the wire
		for idx := len(keys) - 1; idx >= 0; idx-- {
			k := keys[idx]
			v := m.Notifications[k]
			baseI := i
			size, err := v.MarshalToSizedBufferVT(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = protohelpers.EncodeVarint(dAtA, i, uint64(size))
			i--
			dAtA[i] = wireTagMapValue
			i -= len(k)
			copy(dAtA[i:], k)
			i = protohelpers.EncodeVarint(dAtA, i, uint64(len(k)))
			i--
			dAtA[i] = wireTagMapKey
			i = protohelpers.EncodeVarint(dAtA, i, uint64(baseI-i))
			i--
			dAtA[i] = wireTagNotificationsMap
		}
	}
	if m.Timestamp != 0 {
		i -= 8
		binary.LittleEndian.PutUint64(dAtA[i:], m.Timestamp)
		i--
		dAtA[i] = wireTagTimestamp
	}
	if m.Offset != 0 {
		i = protohelpers.EncodeVarint(dAtA, i, uint64(m.Offset))
		i--
		dAtA[i] = wireTagOffset
	}
	if m.Shard != 0 {
		i = protohelpers.EncodeVarint(dAtA, i, uint64(m.Shard))
		i--
		dAtA[i] = wireTagShard
	}
	return len(dAtA) - i, nil
}
