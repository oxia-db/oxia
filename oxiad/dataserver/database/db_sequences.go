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

package database

import (
	"fmt"
	"math"
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/proto"
)

const maxSequence = uint64(math.MaxUint64)

func generateUniqueKeyFromSequences(batch kvstore.WriteBatch, req *proto.PutRequest) (string, error) {
	if req.PartitionKey == nil {
		// All the keys need to be in same shard to guarantee atomicity
		return "", ErrMissingPartitionKey
	}

	if req.ExpectedVersionId != nil {
		// Cannot set an expected version id when key is sequential
		return "", ErrBadVersionId
	}

	parts, err := findCurrentLastKeyInSequence(batch, req)
	if err != nil {
		return "", err
	}

	newKey := req.Key
	for idx, delta := range req.SequenceKeyDelta {
		if idx == 0 && delta == 0 {
			// The first delta in the list must be strictly > 0
			// Otherwise there would be possibility of reordering of keys
			return "", ErrSequenceDeltaIsZero
		}

		var lastValue uint64
		if idx < len(parts) {
			_, err := fmt.Sscanf(parts[idx], "%020d", &lastValue)
			if err != nil {
				return "", err
			}
		} else {
			// There are additional sequences
			lastValue = 0
		}

		newKey = fmt.Sprintf("%s-%020d", newKey, lastValue+delta)
	}

	return newKey, nil
}

func findCurrentLastKeyInSequence(wb kvstore.WriteBatch, req *proto.PutRequest) ([]string, error) {
	prefixKey := req.Key
	maxKey := fmt.Sprintf("%s-%020d", prefixKey, maxSequence)
	lastKeyInSequence, err := wb.FindLower(maxKey)
	if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
		return nil, err
	}

	if errors.Is(err, kvstore.ErrKeyNotFound) || !strings.HasPrefix(lastKeyInSequence, prefixKey) {
		lastKeyInSequence = ""
	} else {
		lastKeyInSequence = strings.TrimPrefix(lastKeyInSequence, prefixKey)
	}

	parts := strings.Split(lastKeyInSequence, "-")[1:]
	if len(parts) > len(req.SequenceKeyDelta) {
		// The request has less sequence key deltas than there are already
		// available in the sequence
		return nil, ErrMissingSequenceDeltas
	}
	return parts, nil
}
