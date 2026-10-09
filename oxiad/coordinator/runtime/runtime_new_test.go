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

package runtime

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"
)

// The data server controllers that New starts may discover a node's features
// while New is still starting the shard controllers: the discovery must not
// observe the shard controllers half-built.
func TestNewDiscoversFeaturesWhileStartingShardControllers(t *testing.T) {
	servers := []*proto.DataServerIdentity{
		{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"},
		{Name: new("s2"), Public: "s2:9091", Internal: "s2:8191"},
		{Name: new("s3"), Public: "s3:9091", Internal: "s3:8191"},
	}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			InitialShardCount: 64,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	})

	const shardCount = 64
	_, err := metadata.AllocateShardIDs(shardCount)
	require.NoError(t, err)
	shards := make(map[int64]*proto.ShardMetadata, shardCount)
	width := uint32(math.MaxUint32 / shardCount)
	for shard := range int64(shardCount) {
		hashRange := &proto.HashRange{Min: uint32(shard) * width, Max: uint32(shard+1)*width - 1}
		if shard == shardCount-1 {
			hashRange.Max = math.MaxUint32
		}
		shards[shard] = &proto.ShardMetadata{
			Status:         proto.ShardStatusSteadyState,
			Term:           1,
			Leader:         servers[shard%3],
			Ensemble:       servers,
			Int32HashRange: hashRange,
		}
	}
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards:            shards,
	}))

	// Each node supports features, so its first handshake is a discovery
	rpcProvider := mockutils.NewRpcProvider()
	for _, server := range servers {
		rpcProvider.GetNode(server).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	}

	r, err := New(metadata, func(string) rpc.Provider { return rpcProvider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	require.Eventually(t, func() bool {
		for _, server := range servers {
			status, ok := r.GetDataServerStatus(server.GetNameOrDefault())
			if !ok || status.GetState() != proto.DataServerState_DATA_SERVER_STATE_RUNNING {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "data servers did not become available")
}
