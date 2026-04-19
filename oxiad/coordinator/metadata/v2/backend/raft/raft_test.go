package raft

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	documentbackend "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/backend"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/document"
	metadataerr "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/error"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestRaftBackendRejectsFollowerWritesAndReplicatesDocuments(t *testing.T) {
	cluster := newTestRaftCluster(t)
	defer func() {
		require.NoError(t, cluster.Close())
	}()

	leaderStore, _ := cluster.Leader(t)
	followerStore := cluster.Follower(t, leaderStore)

	err := followerStore.AddExtraAllowedAuthorities([]string{"should-fail"})
	require.ErrorIs(t, err, metadataerr.ErrLeaseNotHeld)

	const dataServerName = "ds/1"
	const namespaceName = "ns/1"

	require.NoError(t, leaderStore.AddExtraAllowedAuthorities([]string{"a", "", "b", "a"}))
	require.NoError(t, leaderStore.CreateDataServers([]*metadatapb.DataServer{{
		Name:            dataServerName,
		PublicAddress:   "public-1",
		InternalAddress: "internal-1",
	}}))
	require.NoError(t, leaderStore.CreateNamespaces([]*metadatapb.Namespace{{
		Name: namespaceName,
	}}))

	namespaceState, err := leaderStore.PatchNamespaceState(namespaceName, &metadatapb.NamespaceState{
		ReplicationFactor: 3,
		Shards: map[int64]*metadatapb.ShardState{
			1: {
				Status: metadatapb.ShardStatus_SHARD_STATUS_STEADY,
				Term:   7,
				Leader: dataServerName,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint32(3), namespaceState.ReplicationFactor)

	updatedShard, err := leaderStore.PatchShardState(namespaceName, 1, &metadatapb.ShardState{
		Status: metadatapb.ShardStatus_SHARD_STATUS_STEADY,
		Term:   8,
		Leader: dataServerName,
	})
	require.NoError(t, err)
	require.Equal(t, int64(8), updatedShard.Term)

	require.Eventually(t, func() bool {
		if authorities := leaderStore.GetAllowedAuthorities(); !assert.ElementsMatch(t, []string{"a", "b"}, authorities) {
			return false
		}

		dataServer, err := leaderStore.GetDataServer(dataServerName)
		if err != nil || dataServer.PublicAddress != "public-1" {
			return false
		}

		namespace, err := leaderStore.GetNamespace(namespaceName)
		if err != nil || namespace.Name != namespaceName {
			return false
		}

		namespaceState, err := leaderStore.GetNamespaceState(namespaceName)
		if err != nil || namespaceState.ReplicationFactor != 3 {
			return false
		}

		shard, err := leaderStore.GetShardState(namespaceName, 1)
		if err != nil || !proto.Equal(shard, updatedShard) {
			return false
		}

		for _, backend := range cluster.backends {
			configRecord := backend.Load(documentbackend.ConfigRecordName)
			statusRecord := backend.Load(documentbackend.StatusRecordName)

			conf, ok := configRecord.Value.(*metadatapb.Cluster)
			if !ok || !assert.ElementsMatch(t, []string{"a", "b"}, conf.AllowedExtraAuthorities) {
				return false
			}
			replicatedDataServer, ok := conf.DataServers[dataServerName]
			if !ok || replicatedDataServer == nil || replicatedDataServer.PublicAddress != "public-1" {
				return false
			}
			replicatedNamespace, ok := conf.Namespaces[namespaceName]
			if !ok || replicatedNamespace == nil || replicatedNamespace.Name != namespaceName {
				return false
			}
			status, ok := statusRecord.Value.(*metadatapb.ClusterState)
			if !ok {
				return false
			}
			replicatedState, ok := status.Namespaces[namespaceName]
			if !ok || replicatedState == nil || replicatedState.ReplicationFactor != 3 {
				return false
			}
			replicatedShard, ok := replicatedState.Shards[1]
			if !ok || !proto.Equal(replicatedShard, updatedShard) {
				return false
			}
		}
		return true
	}, 20*time.Second, 200*time.Millisecond)
}

func TestRaftBackendStoresWholeDocuments(t *testing.T) {
	cluster := newTestRaftCluster(t)
	defer func() {
		require.NoError(t, cluster.Close())
	}()

	leaderStore, leaderBackend := cluster.Leader(t)

	const dataServerName = "ds/with/slash"
	const namespaceName = "ns/with/slash"

	require.NoError(t, leaderStore.AddExtraAllowedAuthorities([]string{"a"}))
	require.NoError(t, leaderStore.CreateDataServers([]*metadatapb.DataServer{{
		Name:            dataServerName,
		PublicAddress:   "public-2",
		InternalAddress: "internal-2",
	}}))
	require.NoError(t, leaderStore.CreateNamespaces([]*metadatapb.Namespace{{
		Name: namespaceName,
	}}))
	_, err := leaderStore.PatchNamespaceState(namespaceName, &metadatapb.NamespaceState{
		ReplicationFactor: 2,
		Shards: map[int64]*metadatapb.ShardState{
			12: {Leader: dataServerName},
		},
	})
	require.NoError(t, err)

	configRecord := leaderBackend.Load(documentbackend.ConfigRecordName)
	statusRecord := leaderBackend.Load(documentbackend.StatusRecordName)

	require.NotEmpty(t, configRecord.Version)
	require.NotEmpty(t, statusRecord.Version)

	conf, ok := configRecord.Value.(*metadatapb.Cluster)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"a"}, conf.AllowedExtraAuthorities)
	require.Contains(t, conf.DataServers, dataServerName)
	require.Contains(t, conf.Namespaces, namespaceName)

	status, ok := statusRecord.Value.(*metadatapb.ClusterState)
	require.True(t, ok)
	require.Contains(t, status.Namespaces, namespaceName)
	require.Contains(t, status.Namespaces[namespaceName].Shards, int64(12))
}

func TestRaftBackendRejectsBadVersion(t *testing.T) {
	cluster := newTestRaftCluster(t)
	defer func() {
		require.NoError(t, cluster.Close())
	}()

	_, leaderBackend := cluster.Leader(t)

	record := leaderBackend.Load(documentbackend.ConfigRecordName)
	conf, ok := record.Value.(*metadatapb.Cluster)
	require.True(t, ok)
	conf.AllowedExtraAuthorities = []string{"a"}
	require.NoError(t, leaderBackend.Store(documentbackend.ConfigRecordName, record))

	stale := leaderBackend.Load(documentbackend.ConfigRecordName)
	stale.Version = ""
	staleConf, ok := stale.Value.(*metadatapb.Cluster)
	require.True(t, ok)
	staleConf.AllowedExtraAuthorities = []string{"b"}

	err := leaderBackend.Store(documentbackend.ConfigRecordName, stale)
	require.ErrorIs(t, err, document.ErrBadVersion)
}

func TestRaftBackendDeleteNamespaceAndState(t *testing.T) {
	cluster := newTestRaftCluster(t)
	defer func() {
		require.NoError(t, cluster.Close())
	}()

	leaderStore, leaderBackend := cluster.Leader(t)
	const namespaceName = "ns-delete"

	require.NoError(t, leaderStore.CreateNamespaces([]*metadatapb.Namespace{{Name: namespaceName}}))
	_, err := leaderStore.PatchNamespaceState(namespaceName, &metadatapb.NamespaceState{
		ReplicationFactor: 2,
		Shards: map[int64]*metadatapb.ShardState{
			1: {Leader: "l1"},
			2: {Leader: "l2"},
		},
	})
	require.NoError(t, err)

	require.NoError(t, leaderStore.DeleteNamespaces([]string{namespaceName}))
	require.NoError(t, leaderStore.DeleteNamespaceStates([]string{namespaceName}))

	_, err = leaderStore.GetNamespace(namespaceName)
	require.ErrorIs(t, err, metadataerr.ErrNotFound)
	_, err = leaderStore.GetNamespaceState(namespaceName)
	require.ErrorIs(t, err, metadataerr.ErrNotFound)
	_, err = leaderStore.GetShardState(namespaceName, 1)
	require.ErrorIs(t, err, metadataerr.ErrNotFound)

	configRecord := leaderBackend.Load(documentbackend.ConfigRecordName)
	statusRecord := leaderBackend.Load(documentbackend.StatusRecordName)
	conf, ok := configRecord.Value.(*metadatapb.Cluster)
	require.True(t, ok)
	status, ok := statusRecord.Value.(*metadatapb.ClusterState)
	require.True(t, ok)
	assert.NotContains(t, conf.Namespaces, namespaceName)
	assert.NotContains(t, status.Namespaces, namespaceName)
}

type testRaftCluster struct {
	stores   []*document.Store
	backends []*Backend
}

func (c *testRaftCluster) Close() error {
	var closeErr error
	for _, store := range c.stores {
		if err := store.Close(); err != nil {
			closeErr = err
		}
	}
	return closeErr
}

func (c *testRaftCluster) Leader(t *testing.T) (*document.Store, *Backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for {
		for i, backend := range c.backends {
			state, _ := backend.LeaseWatch().Load()
			if state == metadatapb.LeaseState_LEASE_STATE_HELD {
				return c.stores[i], backend
			}
		}

		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for raft leader")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *testRaftCluster) Follower(t *testing.T, leader *document.Store) *document.Store {
	t.Helper()
	for _, store := range c.stores {
		if store != leader {
			return store
		}
	}
	t.Fatal("missing follower")
	return nil
}

func newTestRaftCluster(t *testing.T) *testRaftCluster {
	t.Helper()

	addresses := reserveTCPAddresses(t, 3)
	bootstrapNodes := append([]string(nil), addresses...)
	baseDir := t.TempDir()

	cluster := &testRaftCluster{}
	for i, address := range addresses {
		backend := NewBackend(context.Background(), option.RaftMetadata{
			BootstrapNodes: bootstrapNodes,
			Address:        address,
			DataDir:        filepath.Join(baseDir, t.Name(), time.Now().Format("150405"), fmt.Sprintf("node-%d", i)),
		})
		cluster.backends = append(cluster.backends, backend)
		cluster.stores = append(cluster.stores, document.NewStore(context.Background(), backend))
	}
	return cluster
}

func reserveTCPAddresses(t *testing.T, n int) []string {
	t.Helper()

	listeners := make([]net.Listener, 0, n)
	addresses := make([]string, 0, n)

	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, l)
		addresses = append(addresses, l.Addr().String())
	}

	for _, l := range listeners {
		require.NoError(t, l.Close())
	}
	return addresses
}
