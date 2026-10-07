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
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

// The clients merge the results of the shards of a namespace in the order of
// its keys, so the assignments of each namespace carry its key sorting. The
// data servers keep the keys of a namespace without a key sorting in
// hierarchical order.
func TestComputeNewAssignmentsKeySorting(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "public:6648", Internal: "internal:6649"}
	expected := map[string]proto.KeySorting{
		"natural":      proto.KeySorting_KEY_SORTING_NATURAL,
		"hierarchical": proto.KeySorting_KEY_SORTING_HIERARCHICAL,
		"":             proto.KeySorting_KEY_SORTING_HIERARCHICAL,
	}

	clusterConfig := &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{server}}
	for keySorting := range expected {
		clusterConfig.Namespaces = append(clusterConfig.Namespaces, &proto.Namespace{
			Name:              "ns-" + keySorting,
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        keySorting,
		})
	}
	metadata := newTestMetadata(t, clusterConfig)
	for keySorting := range expected {
		require.NoError(t, metadata.CreateNamespaceStatus("ns-"+keySorting, &proto.NamespaceStatus{
			ReplicationFactor: 1,
			Shards: map[int64]*proto.ShardMetadata{
				0: {
					Status:         proto.ShardStatusSteadyState,
					Leader:         server,
					Ensemble:       []*proto.DataServerIdentity{server},
					Int32HashRange: &proto.HashRange{Min: 0, Max: 100},
				},
			},
		}))
	}
	c := &runtime{
		RWMutex:          sync.RWMutex{},
		ctx:              t.Context(),
		metadata:         metadata,
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}

	c.computeNewAssignments()
	assignments := c.assignmentsWatch.Load()

	for keySorting, expectedKeySorting := range expected {
		nsAssignments, ok := assignments.Namespaces["ns-"+keySorting]
		require.True(t, ok)
		assert.Equal(t, expectedKeySorting, nsAssignments.KeySorting, "key sorting %q", keySorting)
	}
}

type listStatusFailingMetadata struct {
	coordmetadata.Metadata
	failures int
}

func (m *listStatusFailingMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.failures > 0 {
		m.failures--
		return nil, errors.New("status unavailable")
	}
	return m.Metadata.ListNamespaceStatus()
}

func TestComputeNewAssignmentsRetriesStatusLoadError(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "public:6648", Internal: "internal:6649"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{{Name: "ns", InitialShardCount: 1, ReplicationFactor: 1}},
	})
	require.NoError(t, metadata.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Leader:         server,
				Ensemble:       []*proto.DataServerIdentity{server},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 100},
			},
		},
	}))
	failingMetadata := &listStatusFailingMetadata{Metadata: metadata, failures: 2}
	c := &runtime{
		RWMutex:          sync.RWMutex{},
		ctx:              t.Context(),
		logger:           slog.Default(),
		metadata:         failingMetadata,
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}

	c.computeNewAssignments()

	require.Zero(t, failingMetadata.failures)
	nsAssignments, ok := c.assignmentsWatch.Load().Namespaces["ns"]
	require.True(t, ok)
	require.Len(t, nsAssignments.Assignments, 1)
	assert.Equal(t, server.GetPublic(), nsAssignments.Assignments[0].Leader)
}
