package metadata

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/oxia-db/oxia/common/entity"
	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	metadata_v1 "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v1"
	v2backend "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/backend"
	v2file "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/backend/file"
	v2kubernetes "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/backend/kubernetes"
	v2raft "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/backend/raft"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/policy"
)

func TestConvertV1ToV2(t *testing.T) {
	clusterConfig, clusterStatus := sampleV1Metadata()

	configRecord, statusRecord, err := ConvertV1ToV2(clusterConfig, clusterStatus)
	require.NoError(t, err)

	assert.Equal(t, []string{"extra-a.example:6648", "extra-b.example:6648"}, configRecord.AllowedExtraAuthorities)
	require.NotNil(t, configRecord.LoadBalancer)
	assert.Equal(t, 5*time.Second, configRecord.LoadBalancer.ScheduleInterval.AsDuration())
	assert.Equal(t, 2*time.Minute, configRecord.LoadBalancer.QuarantineTime.AsDuration())

	require.Contains(t, configRecord.DataServers, "ds-1")
	assert.Equal(t, "public-1:6648", configRecord.DataServers["ds-1"].PublicAddress)
	assert.Equal(t, map[string]string{"rack": "r1", "zone": "z1"}, configRecord.DataServers["ds-1"].Labels)

	require.Contains(t, configRecord.Namespaces, "alpha")
	alphaNamespace := configRecord.Namespaces["alpha"]
	require.NotNil(t, alphaNamespace.Policies)
	assert.Equal(t, uint32(3), alphaNamespace.Policies.GetInitShardCount())
	assert.Equal(t, uint32(2), alphaNamespace.Policies.GetReplicationFactor())
	assert.False(t, alphaNamespace.Policies.GetNotificationEnabled())
	assert.Equal(t, string(model.KeySortingHierarchical), alphaNamespace.Policies.GetKeySort())
	require.Len(t, alphaNamespace.Policies.GetAntiAffinities(), 2)
	assert.Equal(t, metadatapb.AntiAffinityMode_ANTI_AFFINITY_MODE_STRICT, alphaNamespace.Policies.GetAntiAffinities()[0].Mode)
	assert.Equal(t, metadatapb.AntiAffinityMode_ANTI_AFFINITY_MODE_RELAX, alphaNamespace.Policies.GetAntiAffinities()[1].Mode)

	assert.Equal(t, int64(42), statusRecord.ShardIdGenerator)
	require.Contains(t, statusRecord.Namespaces, "alpha")
	alphaState := statusRecord.Namespaces["alpha"]
	assert.Equal(t, uint32(2), alphaState.ReplicationFactor)
	require.Contains(t, alphaState.Shards, int64(7))

	shard := alphaState.Shards[7]
	assert.Equal(t, metadatapb.ShardStatus_SHARD_STATUS_STEADY, shard.Status)
	assert.Equal(t, int64(9), shard.Term)
	assert.Equal(t, "ds-1", shard.Leader)
	assert.Equal(t, []string{"ds-1", "ds-2"}, shard.Ensemble)
	assert.Equal(t, []string{"ds-3"}, shard.RemovedNodes)
	assert.Equal(t, []string{"ds-2"}, shard.PendingDeleteShardNodes)
	require.NotNil(t, shard.Split)
	assert.Equal(t, metadatapb.SplitPhase_SPLIT_PHASE_CATCH_UP, shard.Split.Phase)
	assert.Equal(t, int64(5), shard.Split.GetParentShardId())
	assert.Equal(t, []int64{7, 8}, shard.Split.ChildShardIds)
	assert.Equal(t, uint32(123), shard.Split.SplitPoint)
	assert.Equal(t, int64(456), shard.Split.GetSnapshotOffset())
	assert.Equal(t, int64(11), shard.Split.GetParentTermAtBootstrap())
	assert.Equal(t, map[int64]string{7: "leader-a", 8: "leader-b"}, shard.Split.ChildLeadersAtBootstrap)
}

func TestMigrateV1ToV2File(t *testing.T) {
	clusterConfig, clusterStatus := sampleV1Metadata()
	expectedConfig, expectedStatus, err := ConvertV1ToV2(clusterConfig, clusterStatus)
	require.NoError(t, err)

	baseDir := t.TempDir()
	sourcePath := filepath.Join(baseDir, "v1", "metadata.json")

	seeder := metadata_v1.NewMetadataProviderFile(sourcePath)
	require.NoError(t, seeder.WaitToBecomeLeader())
	_, err = seeder.Store(clusterStatus, metadata_v1.NotExists)
	require.NoError(t, err)
	require.NoError(t, seeder.Close())

	source := metadata_v1.NewMetadataProviderFile(sourcePath)
	t.Cleanup(func() {
		require.NoError(t, source.Close())
	})

	destDir := filepath.Join(baseDir, "v2")
	require.NoError(t, os.MkdirAll(destDir, 0o755))
	destination := v2file.NewBackend(context.Background(), option.FileMetadata{Path: destDir})
	t.Cleanup(func() {
		require.NoError(t, destination.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, MigrateV1ToV2(ctx, source, destination, clusterConfig, V1ToV2MigrationOptions{}))

	assertMigratedRecords(t, destination, expectedConfig, expectedStatus)
}

func TestMigrateV1ToV2Raft(t *testing.T) {
	clusterConfig, clusterStatus := sampleV1Metadata()
	expectedConfig, expectedStatus, err := ConvertV1ToV2(clusterConfig, clusterStatus)
	require.NoError(t, err)

	addresses := reserveTCPAddresses(t, 2)
	baseDir := t.TempDir()

	source, err := metadata_v1.NewMetadataProviderRaft(addresses[0], []string{addresses[0]}, filepath.Join(baseDir, "v1"))
	require.NoError(t, err)
	require.NoError(t, source.WaitToBecomeLeader())
	_, err = source.Store(clusterStatus, metadata_v1.NotExists)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, source.Close())
	})

	destination := v2raft.NewBackend(context.Background(), option.RaftMetadata{
		BootstrapNodes: []string{addresses[1]},
		Address:        addresses[1],
		DataDir:        filepath.Join(baseDir, "v2"),
	})
	t.Cleanup(func() {
		require.NoError(t, destination.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, MigrateV1ToV2(ctx, source, destination, clusterConfig, V1ToV2MigrationOptions{
		AcquireSourceLeadership: func(context.Context) error {
			return nil
		},
	}))

	assertMigratedRecords(t, destination, expectedConfig, expectedStatus)
}

func TestMigrateV1ToV2KubernetesKind(t *testing.T) {
	if os.Getenv("OXIA_TEST_KIND_K8S") == "" {
		t.Skip("set OXIA_TEST_KIND_K8S=1 to run the kind-backed kubernetes migration test")
	}

	clusterConfig, clusterStatus := sampleV1Metadata()
	expectedConfig, expectedStatus, err := ConvertV1ToV2(clusterConfig, clusterStatus)
	require.NoError(t, err)

	client := metadata_v1.NewK8SClientset(metadata_v1.NewK8SClientConfig())
	namespace := fmt.Sprintf("oxia-metadata-migrate-%d", time.Now().UnixNano())

	_, err = client.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := client.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			require.NoError(t, err)
		}
	})

	sourceName := "legacy-metadata"
	seeder := metadata_v1.NewMetadataProviderConfigMap(client, namespace, sourceName)
	require.NoError(t, seeder.WaitToBecomeLeader())
	_, err = seeder.Store(clusterStatus, metadata_v1.NotExists)
	require.NoError(t, err)
	require.NoError(t, seeder.Close())

	source := metadata_v1.NewMetadataProviderConfigMap(client, namespace, sourceName)
	t.Cleanup(func() {
		require.NoError(t, source.Close())
	})

	destination := v2kubernetes.NewBackend(context.Background(), option.K8sMetadata{
		Namespace: namespace,
	})
	t.Cleanup(func() {
		require.NoError(t, destination.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, MigrateV1ToV2(ctx, source, destination, clusterConfig, V1ToV2MigrationOptions{}))

	assertMigratedRecords(t, destination, expectedConfig, expectedStatus)
}

func assertMigratedRecords(
	t *testing.T,
	destination v2backend.Backend,
	expectedConfig *metadatapb.Cluster,
	expectedStatus *metadatapb.ClusterState,
) {
	t.Helper()

	configRecord := destination.Load(v2backend.ConfigRecordName)
	require.NotNil(t, configRecord)
	configValue, ok := configRecord.Value.(*metadatapb.Cluster)
	require.True(t, ok)
	assert.True(t, gproto.Equal(expectedConfig, configValue))

	statusRecord := destination.Load(v2backend.StatusRecordName)
	require.NotNil(t, statusRecord)
	statusValue, ok := statusRecord.Value.(*metadatapb.ClusterState)
	require.True(t, ok)
	assert.True(t, gproto.Equal(expectedStatus, statusValue))
}

func sampleV1Metadata() (*model.ClusterConfig, *model.ClusterStatus) {
	ds1Name := "ds-1"
	ds2Name := "ds-2"
	ds3Name := "ds-3"

	ds1 := model.Server{Name: &ds1Name, Public: "public-1:6648", Internal: "internal-1:6649"}
	ds2 := model.Server{Name: &ds2Name, Public: "public-2:6648", Internal: "internal-2:6649"}
	ds3 := model.Server{Name: &ds3Name, Public: "public-3:6648", Internal: "internal-3:6649"}

	clusterConfig := &model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:                 "alpha",
				InitialShardCount:    3,
				ReplicationFactor:    2,
				NotificationsEnabled: entity.Bool(false),
				KeySorting:           model.KeySortingHierarchical,
				Policies: &policy.Policies{
					AntiAffinities: []policy.AntiAffinity{
						{Labels: []string{"zone", "rack"}, Mode: policy.Strict},
						{Labels: []string{"disk"}, Mode: policy.Relaxed},
					},
				},
			},
			{
				Name:                 "beta",
				InitialShardCount:    1,
				ReplicationFactor:    1,
				NotificationsEnabled: entity.Bool(true),
				KeySorting:           model.KeySortingNatural,
			},
		},
		Servers:               []model.Server{ds1, ds2, ds3},
		AllowExtraAuthorities: []string{"extra-a.example:6648", "extra-b.example:6648"},
		ServerMetadata: map[string]model.ServerMetadata{
			"ds-1": {Labels: map[string]string{"rack": "r1", "zone": "z1"}},
			"ds-2": {Labels: map[string]string{"rack": "r2", "zone": "z2"}},
			"ds-3": {Labels: map[string]string{"rack": "r3", "zone": "z3"}},
		},
		LoadBalancer: &model.LoadBalancer{
			ScheduleInterval: 5 * time.Second,
			QuarantineTime:   2 * time.Minute,
		},
	}

	clusterStatus := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"alpha": {
				ReplicationFactor: 2,
				Shards: map[int64]model.ShardMetadata{
					7: {
						Status:                  model.ShardStatusSteadyState,
						Term:                    9,
						Leader:                  &ds1,
						Ensemble:                []model.Server{ds1, ds2},
						RemovedNodes:            []model.Server{ds3},
						PendingDeleteShardNodes: []model.Server{ds2},
						Int32HashRange:          model.Int32HashRange{Min: 10, Max: 20},
						Split: &model.SplitMetadata{
							Phase:                 model.SplitPhaseCatchUp,
							ParentShardId:         5,
							ChildShardIDs:         []int64{7, 8},
							SplitPoint:            123,
							SnapshotOffset:        456,
							ParentTermAtBootstrap: 11,
							ChildLeadersAtBootstrap: map[int64]string{
								7: "leader-a",
								8: "leader-b",
							},
						},
					},
					8: {
						Status:         model.ShardStatusElection,
						Term:           10,
						Leader:         &ds2,
						Ensemble:       []model.Server{ds2, ds3},
						Int32HashRange: model.Int32HashRange{Min: 21, Max: 30},
					},
				},
			},
			"beta": {
				ReplicationFactor: 1,
				Shards:            map[int64]model.ShardMetadata{},
			},
		},
		ShardIdGenerator: 42,
		ServerIdx:        99,
	}

	return clusterConfig, clusterStatus
}

func reserveTCPAddresses(t *testing.T, n int) []string {
	t.Helper()

	listeners := make([]net.Listener, 0, n)
	addresses := make([]string, 0, n)

	for i := 0; i < n; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}

	for _, listener := range listeners {
		require.NoError(t, listener.Close())
	}
	return addresses
}
