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
	"time"

	"github.com/stretchr/testify/require"

	commonobject "github.com/oxia-db/oxia/common/object"
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

type namespaceReadHookMetadata struct {
	coordmetadata.Metadata
	afterRead func()
}

func (m *namespaceReadHookMetadata) GetNamespaceStatus(name string) (commonobject.Borrowed[*proto.NamespaceStatus], bool) {
	status, exists := m.Metadata.GetNamespaceStatus(name)
	if hook := m.afterRead; hook != nil {
		m.afterRead = nil
		hook()
	}
	return status, exists
}

// committingNamespaceMetadata persists through the real metadata store before
// reporting an ambiguous write error, or a competing namespace creation.
type committingNamespaceMetadata struct {
	coordmetadata.Metadata
	err          error
	saved        *proto.NamespaceStatus
	allocations int
}

func (m *committingNamespaceMetadata) AllocateShardIDs(count uint32) (int64, error) {
	m.allocations++
	return m.Metadata.AllocateShardIDs(count)
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

func TestCreateNamespaceRepairsCommittedWrite(t *testing.T) {
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

	require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), storeErr)
	require.Empty(t, c.shardControllers)
	saved, exists := metadata.GetNamespaceStatus(namespace.Name)
	require.True(t, exists)
	require.Len(t, saved.UnsafeBorrow().Shards, 2)
	require.Equal(t, 1, metadata.allocations)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	controllers := maps.Clone(c.shardControllers)
	c.RUnlock()
	require.Len(t, controllers, 2)
	for shard := range saved.UnsafeBorrow().Shards {
		require.Contains(t, controllers, shard)
	}

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
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
	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.Len(t, c.shardControllers, 2)
	require.NotSame(t, controllers[0], c.shardControllers[0])
	require.Same(t, controllers[1], c.shardControllers[1])
	require.Equal(t, 1, metadata.allocations)
}

func TestCreateNamespaceRepairsAlreadyExistsOnRetry(t *testing.T) {
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

	require.Same(t, metadata.err, c.CreateNamespace(namespace.Name, namespace))
	require.Empty(t, c.shardControllers)
	require.Equal(t, 1, metadata.allocations)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.Len(t, c.shardControllers, 1)
	require.Contains(t, c.shardControllers, int64(42))
	require.NotContains(t, c.shardControllers, int64(0))
	require.Equal(t, 1, metadata.allocations)
}

func TestCreateNamespaceDoesNotReviveDeletedShards(t *testing.T) {
	for _, count := range []uint32{1, 2} {
		t.Run(fmt.Sprintf("%d shards", count), func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
			namespace := &proto.Namespace{Name: "default", InitialShardCount: count, ReplicationFactor: 1}
			metadata := &namespaceReadHookMetadata{
				Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
					Servers:    []*proto.DataServerIdentity{server},
					Namespaces: []*proto.Namespace{namespace},
				}),
			}
			c := newNamespaceRuntimeForCreation(t, metadata)
			require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
			// Wait for the original controllers to start before deleting their status.
			requests := c.rpc.(*mockutils.RpcProvider).GetNode(server).NewTermRequests
			for range count {
				select {
				case <-requests:
				case <-time.After(5 * time.Second):
					t.Fatal("initial shard controller did not start")
				}
			}

			// Remove the shard and its controller after the initial status read,
			// while returning the old snapshot to CreateNamespace.
			metadata.afterRead = func() {
				require.NoError(t, metadata.DeleteShardStatus(namespace.Name, 0))
				c.ShardDeleted(0)
			}
			err := c.CreateNamespace(namespace.Name, namespace)
			if count == 1 {
				require.ErrorIs(t, err, metadatacommon.ErrConflict)
				require.Contains(t, err.Error(), namespace.Name)
			} else {
				require.NoError(t, err)
			}
			c.RLock()
			defer c.RUnlock()
			require.NotContains(t, c.shardControllers, int64(0))
			require.Len(t, c.shardControllers, int(count)-1)
		})
	}
}

func TestCreateNamespaceSkipsDeletingShards(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 2, ReplicationFactor: 1}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	require.NoError(t, metadata.CreateNamespaceStatus(namespace.Name, &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {Status: proto.ShardStatusDeleting, Ensemble: []*proto.DataServerIdentity{server}},
			1: {Status: proto.ShardStatusUnknown, Ensemble: []*proto.DataServerIdentity{server}},
		},
	}))
	c := newNamespaceRuntimeForCreation(t, metadata)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.NotContains(t, c.shardControllers, int64(0))
	require.Contains(t, c.shardControllers, int64(1))
}

func (m *failingNamespaceMetadata) CreateNamespaceStatus(_ string, status *proto.NamespaceStatus) error {
	m.proposed = status
	return m.err
}

type namespaceEnsembleSelector struct{}

func (namespaceEnsembleSelector) Select(ctx *ensemble.Context) ([]string, error) {
	return ctx.Candidates.Values(), nil
}

func TestCreateNamespaceHandlesStatusErrors(t *testing.T) {
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

			require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), tc.err)
			require.NotNil(t, metadata.proposed)
			require.Len(t, metadata.proposed.Shards, 1)
			require.Empty(t, c.shardControllers)
			_, exists := metadata.GetNamespaceStatus(namespace.Name)
			require.False(t, exists)
		})
	}
}
