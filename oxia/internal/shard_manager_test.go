// Copyright 2023 StreamNative, Inc.
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

package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/server"
)

type testShardStrategy struct {
}

func (s *testShardStrategy) Get(key string) func(Shard) bool {
	return func(shard Shard) bool {
		return shard.Id%2 == 0
	}
}

func TestWithStandalone(t *testing.T) {
	standaloneServer, err := server.NewStandalone(server.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	clientPool := common.NewClientPool(nil)
	serviceAddress := fmt.Sprintf("localhost:%d", standaloneServer.RpcPort())
	shardManager, err := NewShardManager(&testShardStrategy{}, clientPool, serviceAddress, common.DefaultNamespace, 30*time.Second)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, shardManager.Close())
	}()

	shardId := shardManager.Get("foo")

	assert.EqualValues(t, 0, shardId)
}

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{hashRange(1, 2), hashRange(3, 6), false},
		{hashRange(1, 4), hashRange(3, 6), true},
		{hashRange(4, 5), hashRange(3, 6), true},
		{hashRange(5, 8), hashRange(3, 6), true},
		{hashRange(7, 8), hashRange(3, 6), false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}
