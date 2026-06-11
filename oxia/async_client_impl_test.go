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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

// The first shard error terminates the result channel; the responses of the
// remaining shards — errors included — must be discarded, not panic with a
// send on the closed channel (the error path used to fall through and leave
// the counter sentinel negative, defeating the response-already-sent guard).
func TestMultiShardGetCallback_ErrorsAfterFirstAreDiscarded(t *testing.T) {
	ch := make(chan GetResult, 1)
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, 3, ch)

	callback(nil, errors.New("shard-0 failed"))
	result := <-ch
	require.Error(t, result.Err)

	assert.NotPanics(t, func() {
		callback(nil, errors.New("shard-1 failed"))
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	})

	// The channel was closed exactly once, after the single error result
	_, open := <-ch
	assert.False(t, open)
}

func TestMultiShardGetCallback_AllShardsRespond(t *testing.T) {
	ch := make(chan GetResult, 1)
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, 3, ch)

	for i := 0; i < 3; i++ {
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	}

	result := <-ch
	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
	_, open := <-ch
	assert.False(t, open)
}
