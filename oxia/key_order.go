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

package oxia

import (
	"bytes"

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/common/proto"
)

// keyOrder is the order of the keys in the shards of a namespace. Each shard
// returns the results of a read in this order, and the client picks or merges
// the results of the shards in the same order.
type keyOrder struct {
	// The encoder of the keys in the shards, or nil for the order of the old
	// storage format, compare.CompareWithSlash
	encoder compare.Encoder
}

func newKeyOrder(keySorting proto.KeySorting) keyOrder {
	switch keySorting {
	case proto.KeySorting_KEY_SORTING_NATURAL:
		return keyOrder{encoder: compare.EncoderNatural}
	case proto.KeySorting_KEY_SORTING_HIERARCHICAL:
		return keyOrder{encoder: compare.EncoderHierarchical}
	default:
		// Older servers don't report the key sorting: keep the order that the
		// client has always used
		return keyOrder{}
	}
}

// sortKey returns the form of a key that compareSortKeys orders.
func (o keyOrder) sortKey(key string) []byte {
	if o.encoder == nil {
		return []byte(key)
	}
	return o.encoder.Encode(key)
}

func (o keyOrder) compareSortKeys(a, b []byte) int {
	if o.encoder == nil {
		return compare.CompareWithSlash(a, b)
	}
	return bytes.Compare(a, b)
}

func (o keyOrder) compare(a, b string) int {
	return o.compareSortKeys(o.sortKey(a), o.sortKey(b))
}
