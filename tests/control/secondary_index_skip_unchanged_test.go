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

package control

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

// With FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED, an overwrite writes only the
// index entries that change. The write batches feed the DB checksum: the
// followers must apply the overwrites like the leader and reach its checksum.
func TestSecondaryIndexSkipUnchanged(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	s3, sa3 := mock.NewServer(t, "s3")
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}
	defer func() {
		for _, s := range servers {
			assert.NoError(t, s.Close())
		}
	}()

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   newDefaultClusterConfig(sa1, sa2, sa3),
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc.NewRpcProviderFactory(nil))
	defer coordinatorInstance.Close()

	// All the data servers support the feature, so the coordinator enables it
	shardMetadata := waitForLeaderFeature(t, coordinatorInstance.Metadata(), servers,
		proto.Feature_FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED)
	leader := shardMetadata.Leader.GetNameOrDefault()
	lead, err := servers[leader].GetShardDirector().GetLeader(0)
	require.NoError(t, err)

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace(constant.DefaultNamespace))
	require.NoError(t, err)
	defer client.Close()

	// The overwrites keep both indexes, change one, then drop the other
	for _, indexes := range [][]oxia.PutOption{
		{oxia.SecondaryIndex("by-name", "a"), oxia.SecondaryIndex("by-group", "g")},
		{oxia.SecondaryIndex("by-name", "a"), oxia.SecondaryIndex("by-group", "g")},
		{oxia.SecondaryIndex("by-name", "b"), oxia.SecondaryIndex("by-group", "g")},
		{oxia.SecondaryIndex("by-name", "b")},
	} {
		_, _, err = client.Put(t.Context(), "/record", []byte("v"), indexes...)
		require.NoError(t, err)
	}

	listIndex := func(indexName string, start, end string) []string {
		keys, err := client.List(t.Context(), start, end, oxia.UseIndex(indexName))
		require.NoError(t, err)
		return keys
	}
	assert.Empty(t, listIndex("by-name", "a", "b"))
	assert.Equal(t, []string{"/record"}, listIndex("by-name", "b", "c"))
	assert.Empty(t, listIndex("by-group", "a", "z"))

	// Commit offsets are piggybacked on the replication messages: after one
	// more write the followers know that the overwrites are committed
	leaderChecksum := lead.Checksum().Value()
	assert.NotZero(t, leaderChecksum)
	_, _, err = client.Put(t.Context(), "/flush", []byte("v"))
	require.NoError(t, err)

	for name, s := range servers {
		if name == leader {
			continue
		}
		assert.Eventually(t, func() bool {
			follow, err := s.GetShardDirector().GetFollower(0)
			return err == nil &&
				follow.IsFeatureEnabled(proto.Feature_FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED) &&
				follow.Checksum().Value() == leaderChecksum
		}, 10*time.Second, 100*time.Millisecond, "follower %s did not reach the leader's checksum", name)
	}
}
