package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
)

func TestFileStorePersistsConfigAndStatus(t *testing.T) {
	dir := t.TempDir()

	store := NewStore(context.Background(), dir)
	storeClosed := false
	t.Cleanup(func() {
		if !storeClosed {
			assert.NoError(t, store.Close())
		}
	})

	waitForLeaseState(t, store, metadatapb.LeaseState_LEASE_STATE_HELD)

	require.NoError(t, store.AddExtraAllowedAuthorities([]string{"ca-1"}))
	require.NoError(t, store.CreateDataServers([]*metadatapb.DataServer{
		{
			Name:            "ds-1",
			PublicAddress:   "public:6648",
			InternalAddress: "internal:6649",
		},
	}))
	require.NoError(t, store.CreateNamespaces([]*metadatapb.Namespace{
		{Name: "ns-1"},
	}))
	require.NoError(t, store.CreateNamespaceStates(map[string]*metadatapb.NamespaceState{
		"ns-1": {ReplicationFactor: 3},
	}))

	shard, err := store.PatchShardState("ns-1", 1, &metadatapb.ShardState{
		Status: metadatapb.ShardStatus_SHARD_STATUS_STEADY,
		Term:   7,
		Leader: "ds-1",
	})
	require.NoError(t, err)
	assert.Equal(t, metadatapb.ShardStatus_SHARD_STATUS_STEADY, shard.Status)
	assert.Equal(t, int64(7), shard.Term)

	configContent, err := os.ReadFile(filepath.Join(dir, defaultConfigFileName))
	require.NoError(t, err)
	assert.Contains(t, string(configContent), "allowedExtraAuthorities")
	assert.NotContains(t, string(configContent), "allowed_extra_authorities")

	require.FileExists(t, filepath.Join(dir, defaultConfigFileName))
	require.FileExists(t, filepath.Join(dir, defaultStateFileName))
	require.NoError(t, store.Close())
	storeClosed = true

	reopened := NewStore(context.Background(), dir)
	t.Cleanup(func() {
		assert.NoError(t, reopened.Close())
	})

	waitForLeaseState(t, reopened, metadatapb.LeaseState_LEASE_STATE_HELD)

	assert.Equal(t, []string{"ca-1"}, reopened.GetAllowedAuthorities())

	dataServer, err := reopened.GetDataServer("ds-1")
	require.NoError(t, err)
	assert.Equal(t, "public:6648", dataServer.PublicAddress)
	assert.Equal(t, "internal:6649", dataServer.InternalAddress)

	namespace, err := reopened.GetNamespace("ns-1")
	require.NoError(t, err)
	assert.Equal(t, "ns-1", namespace.Name)

	namespaceState, err := reopened.GetNamespaceState("ns-1")
	require.NoError(t, err)
	assert.EqualValues(t, 3, namespaceState.ReplicationFactor)

	reloadedShard, err := reopened.GetShardState("ns-1", 1)
	require.NoError(t, err)
	assert.Equal(t, metadatapb.ShardStatus_SHARD_STATUS_STEADY, reloadedShard.Status)
	assert.Equal(t, int64(7), reloadedShard.Term)
	assert.Equal(t, "ds-1", reloadedShard.Leader)
}

func TestFileStoreLeaseHandoff(t *testing.T) {
	dir := t.TempDir()

	primary := NewStore(context.Background(), dir)
	primaryClosed := false
	t.Cleanup(func() {
		if !primaryClosed {
			assert.NoError(t, primary.Close())
		}
	})

	waitForLeaseState(t, primary, metadatapb.LeaseState_LEASE_STATE_HELD)
	require.NoError(t, primary.CreateNamespaces([]*metadatapb.Namespace{{Name: "primary"}}))

	secondary := NewStore(context.Background(), dir)
	t.Cleanup(func() {
		assert.NoError(t, secondary.Close())
	})

	state, _ := secondary.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
	assert.ErrorIs(t, secondary.CreateNamespaces([]*metadatapb.Namespace{{Name: "secondary"}}), metadata_v2.ErrLeaseNotHeld)

	require.NoError(t, primary.Close())
	primaryClosed = true

	waitForLeaseState(t, secondary, metadatapb.LeaseState_LEASE_STATE_HELD)

	require.NoError(t, secondary.CreateNamespaces([]*metadatapb.Namespace{{Name: "secondary"}}))
	created, err := secondary.GetNamespace("secondary")
	require.NoError(t, err)
	assert.Equal(t, "secondary", created.Name)

	content, err := os.ReadFile(filepath.Join(dir, defaultConfigFileName))
	require.NoError(t, err)
	assert.Contains(t, string(content), "secondary")
}

func TestFileStoreCloseWhileWaitingForLease(t *testing.T) {
	dir := t.TempDir()

	primary := NewStore(context.Background(), dir)
	t.Cleanup(func() {
		assert.NoError(t, primary.Close())
	})

	waitForLeaseState(t, primary, metadatapb.LeaseState_LEASE_STATE_HELD)

	secondary := NewStore(context.Background(), dir)

	closed := make(chan error, 1)
	go func() {
		closed <- secondary.Close()
	}()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("secondary close blocked while waiting for lease")
	}
}

func waitForLeaseState(t *testing.T, store *Store, expected metadatapb.LeaseState) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watch := store.LeaseWatch()
	state, version := watch.Load()
	for state != expected {
		var err error
		state, version, err = watch.Wait(ctx, version)
		require.NoError(t, err)
	}
}
