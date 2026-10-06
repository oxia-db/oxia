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

package internal

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

// A shard without a leader, e.g. while one is being elected.
type noLeaderShardManager struct {
	ShardManager
}

func (noLeaderShardManager) Leader(int64) string { return "" }

// Records the targets it is asked to connect to.
type recordingClientPool struct {
	rpc.ClientPool
	sync.Mutex
	targets []string
}

func (p *recordingClientPool) GetClientRpc(target string) (proto.OxiaClientClient, error) {
	p.Lock()
	p.targets = append(p.targets, target)
	p.Unlock()
	return p.ClientPool.GetClientRpc(target)
}

// The empty leader address of a shard without a leader is not dialed: the
// pooled connection to it is closed by its failed health ping, and a request
// on it then fails with a Canceled status, which is not retried.
func TestExecutorShardWithoutLeader(t *testing.T) {
	pool := &recordingClientPool{ClientPool: rpc.NewClientPool(nil, nil)}
	defer pool.Close()
	executor := NewExecutor(context.Background(), "default", pool, noLeaderShardManager{}, "localhost:6648")
	shardId := int64(0)

	_, err := executor.ExecuteWrite(context.Background(), &proto.WriteRequest{Shard: &shardId}, nil)
	assert.Equal(t, codes.Unavailable, status.Code(err), "unexpected error: %v", err)

	_, err = executor.ExecuteRead(context.Background(), &proto.ReadRequest{Shard: &shardId}, nil)
	assert.Equal(t, codes.Unavailable, status.Code(err), "unexpected error: %v", err)

	assert.Empty(t, pool.targets)
}
