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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/ensemble"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/shard"
)

type failingNamespaceMetadata struct {
	coordmetadata.Metadata
	err      error
	proposed *proto.NamespaceStatus
}

// committingNamespaceMetadata persists through the real metadata store before
// reporting an ambiguous write error, or a competing namespace creation.
type committingNamespaceMetadata struct {
	coordmetadata.Metadata
	err          error
	saved        *proto.NamespaceStatus
	reservations int
}

func (m *committingNamespaceMetadata) ReserveShardIDs(count uint32) (int64, error) {
	m.reservations++
	return m.Metadata.ReserveShardIDs(count)
}

func (m *committingNamespaceMetadata) CreateNamespaceStatus(name string, status *proto.NamespaceStatus) error {
	if m.saved != nil {
		status = m.saved
	}
	if err := m.Metadata.CreateNamespaceStatus(name, status); err != nil {
		return err
	}
	return m.err
}

func newNamespaceRuntimeForCreation(t *testing.T, metadata coordmetadata.Metadata) *runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	c := &runtime{
		ctx:              ctx,
		ctxCancel:        cancel,
		logger:           slog.Default(),
		metadata:         metadata,
		ensembleSelector: namespaceEnsembleSelector{},
		shardControllers: make(map[int64]shardcontroller.Controller),
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
		rpc:              mockutils.NewRpcProvider(),
	}
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func TestEnsureNamespaceRepairsCommittedWrite(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 2, ReplicationFactor: 1}
	storeErr := errors.New("metadata write timed out after committing")
	metadata := &committingNamespaceMetadata{
		Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
			Servers:    []*proto.DataServerIdentity{server},
			Namespaces: []*proto.Namespace{namespace},
		}),
		err: storeErr,
	}
	c := newNamespaceRuntimeForCreation(t, metadata)

	require.ErrorIs(t, c.EnsureNamespace(namespace.Name, namespace), storeErr)
	require.Empty(t, c.shardControllers)
	saved, exists := metadata.GetNamespaceStatus(namespace.Name)
	require.True(t, exists)
	require.Len(t, saved.UnsafeBorrow().Shards, 2)
	require.Equal(t, 1, metadata.reservations)

	require.NoError(t, c.EnsureNamespace(namespace.Name, namespace))
	c.RLock()
	controllers := maps.Clone(c.shardControllers)
	c.RUnlock()
	require.Len(t, controllers, 2)
	for shard := range saved.UnsafeBorrow().Shards {
		require.Contains(t, controllers, shard)
	}

	require.NoError(t, c.EnsureNamespace(namespace.Name, namespace))
	c.RLock()
	repeated := maps.Clone(c.shardControllers)
	c.RUnlock()
	for shard, controller := range controllers {
		require.Same(t, controller, repeated[shard])
	}

	// Repair a partially initialized namespace without replacing its other controller.
	c.Lock()
	delete(c.shardControllers, 0)
	c.Unlock()
	require.NoError(t, controllers[0].Close())
	require.NoError(t, c.EnsureNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.Len(t, c.shardControllers, 2)
	require.NotSame(t, controllers[0], c.shardControllers[0])
	require.Same(t, controllers[1], c.shardControllers[1])
	require.Equal(t, 1, metadata.reservations)
}

func TestEnsureNamespaceUsesSavedStatusOnAlreadyExists(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
	metadata := &committingNamespaceMetadata{
		Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
			Servers:    []*proto.DataServerIdentity{server},
			Namespaces: []*proto.Namespace{namespace},
		}),
		err: fmt.Errorf("competing creation: %w", metadatacommon.ErrAlreadyExists),
		saved: &proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			42: {Term: 7, Ensemble: []*proto.DataServerIdentity{server}},
		}},
	}
	c := newNamespaceRuntimeForCreation(t, metadata)

	require.NoError(t, c.EnsureNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.Len(t, c.shardControllers, 1)
	require.Contains(t, c.shardControllers, int64(42))
	require.NotContains(t, c.shardControllers, int64(0))
}

func (m *failingNamespaceMetadata) CreateNamespaceStatus(_ string, status *proto.NamespaceStatus) error {
	m.proposed = status
	return m.err
}

type namespaceEnsembleSelector struct{}

func (namespaceEnsembleSelector) Select(ctx *ensemble.Context) ([]string, error) {
	return ctx.Candidates.Values(), nil
}

func TestEnsureNamespaceHandlesStatusErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"already exists", fmt.Errorf("namespace creation: %w", metadatacommon.ErrAlreadyExists)},
		{"write failed", errors.New("status store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
			namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
			metadata := &failingNamespaceMetadata{
				Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
					Servers:    []*proto.DataServerIdentity{server},
					Namespaces: []*proto.Namespace{namespace},
				}),
				err: tc.err,
			}
			c := &runtime{
				ctx:              t.Context(),
				logger:           slog.Default(),
				metadata:         metadata,
				ensembleSelector: namespaceEnsembleSelector{},
				shardControllers: make(map[int64]shardcontroller.Controller),
			}

			require.ErrorIs(t, c.EnsureNamespace(namespace.Name, namespace), tc.err)
			require.NotNil(t, metadata.proposed)
			require.Len(t, metadata.proposed.Shards, 1)
			require.Empty(t, c.shardControllers)
			_, exists := metadata.GetNamespaceStatus(namespace.Name)
			require.False(t, exists)
		})
	}
}
