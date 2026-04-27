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

package coordinator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/common/concurrent"
)

func TestComputeNewAssignmentsIncludesExtraAuthorities(t *testing.T) {
	leader := &proto.DataServerIdentity{
		Public:   "leader-public:6648",
		Internal: "leader-internal:6649",
	}

	clusterConfig := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []*proto.DataServerIdentity{{
			Public:   leader.Public,
			Internal: leader.Internal,
		}},
		AllowExtraAuthorities: []string{
			"bootstrap:6648",
			leader.Public,
		},
	}
	metadata := coordmetadata.New(
		t.Context(),
		memory.NewProvider(provider.ClusterStatusCodec),
		func() (*proto.ClusterConfiguration, error) { return clusterConfig, nil },
		nil,
	)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})
	metadata.UpdateStatus(&proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"default": {
				ReplicationFactor: 1,
				Shards: map[int64]*proto.ShardMetadata{
					0: {
						Status:   proto.ShardStatusUnknown,
						Leader:   leader,
						Ensemble: []*proto.DataServerIdentity{leader},
						Int32HashRange: &proto.HashRange{
							Min: 0,
							Max: 100,
						},
					},
				},
			},
		},
	})
	c := &coordinator{
		RWMutex:            sync.RWMutex{},
		metadata:           metadata,
		assignmentsChanged: concurrent.NewConditionContext(&sync.Mutex{}),
	}

	c.computeNewAssignments()

	nsAssignments, ok := c.assignments.Namespaces["default"]
	require.True(t, ok)
	require.Len(t, nsAssignments.Assignments, 1)
	assert.Equal(t,
		[]string{"leader-public:6648", "leader-internal:6649", "bootstrap:6648"},
		c.assignments.AllowedAuthorities,
	)
}

func TestComputeNewAssignmentsKeepsRemovedShardNodeAuthorities(t *testing.T) {
	active := &proto.DataServerIdentity{
		Public:   "active-public:6648",
		Internal: "active-internal:6649",
	}
	removed := &proto.DataServerIdentity{
		Public:   "removed-public:6648",
		Internal: "removed-internal:6649",
	}

	clusterConfig := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []*proto.DataServerIdentity{{
			Public:   active.Public,
			Internal: active.Internal,
		}},
	}
	metadata := coordmetadata.New(
		t.Context(),
		memory.NewProvider(provider.ClusterStatusCodec),
		func() (*proto.ClusterConfiguration, error) { return clusterConfig, nil },
		nil,
	)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})
	metadata.UpdateStatus(&proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"default": {
				ReplicationFactor: 1,
				Shards: map[int64]*proto.ShardMetadata{
					0: {
						Status:       proto.ShardStatusUnknown,
						Leader:       removed,
						Ensemble:     []*proto.DataServerIdentity{removed},
						RemovedNodes: []*proto.DataServerIdentity{removed},
						Int32HashRange: &proto.HashRange{
							Min: 0,
							Max: 100,
						},
					},
				},
			},
		},
	})
	c := &coordinator{
		RWMutex:            sync.RWMutex{},
		metadata:           metadata,
		assignmentsChanged: concurrent.NewConditionContext(&sync.Mutex{}),
	}

	c.computeNewAssignments()

	assert.Equal(t,
		[]string{
			"active-public:6648",
			"active-internal:6649",
			"removed-public:6648",
			"removed-internal:6649",
		},
		c.assignments.AllowedAuthorities,
	)
}
