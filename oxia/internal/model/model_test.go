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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteCallPartitionKey(t *testing.T) {
	partitionKey := "partition-key"
	call := DeleteCall{
		Key:          "key",
		PartitionKey: &partitionKey,
	}

	assert.Equal(t, partitionKey, call.PartitionKeyOrKey())
	req := call.ToProto()
	assert.NotNil(t, req.PartitionKey)
	assert.Equal(t, partitionKey, req.GetPartitionKey())
}

func TestDeleteCallPartitionKeyOrKeyFallback(t *testing.T) {
	call := DeleteCall{Key: "key"}

	assert.Equal(t, "key", call.PartitionKeyOrKey())
	assert.Nil(t, call.ToProto().PartitionKey)
}

func TestDeleteCallEmptyPartitionKey(t *testing.T) {
	partitionKey := ""
	call := DeleteCall{
		Key:          "key",
		PartitionKey: &partitionKey,
	}

	assert.Equal(t, partitionKey, call.PartitionKeyOrKey())
	req := call.ToProto()
	assert.NotNil(t, req.PartitionKey)
	assert.Equal(t, partitionKey, req.GetPartitionKey())
}

func TestGetCallPartitionKey(t *testing.T) {
	partitionKey := "partition-key"
	call := GetCall{
		Key:          "key",
		PartitionKey: &partitionKey,
	}

	assert.Equal(t, partitionKey, call.PartitionKeyOrKey())
}

func TestGetCallPartitionKeyOrKeyFallback(t *testing.T) {
	call := GetCall{Key: "key"}

	assert.Equal(t, "key", call.PartitionKeyOrKey())
}

func TestGetCallEmptyPartitionKey(t *testing.T) {
	partitionKey := ""
	call := GetCall{
		Key:          "key",
		PartitionKey: &partitionKey,
	}

	assert.Equal(t, partitionKey, call.PartitionKeyOrKey())
}

func TestInOpIndexOrder(t *testing.T) {
	calls := InOpIndexOrder(
		[]PutCall{{Key: "put-0", OpIndex: 0}, {Key: "put-2", OpIndex: 2}},
		[]DeleteCall{{Key: "delete-1", OpIndex: 1}, {Key: "delete-4", OpIndex: 4}},
		[]DeleteRangeCall{{MinKeyInclusive: "range-3", OpIndex: 3}},
	)

	assert.Equal(t, []any{
		PutCall{Key: "put-0", OpIndex: 0},
		DeleteCall{Key: "delete-1", OpIndex: 1},
		PutCall{Key: "put-2", OpIndex: 2},
		DeleteRangeCall{MinKeyInclusive: "range-3", OpIndex: 3},
		DeleteCall{Key: "delete-4", OpIndex: 4},
	}, calls)

	assert.Empty(t, InOpIndexOrder(nil, nil, nil))
}
