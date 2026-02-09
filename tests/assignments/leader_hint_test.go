package assignments

import (
	"testing"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	clientrpc "github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/tests/mock"
	"github.com/stretchr/testify/assert"
)

func TestLeaderHintWithoutClient(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	s3, sa3 := mock.NewServer(t, "s3")
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc.NewRpcProvider(clientrpc.NewClientPool(nil, nil)))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	assert.Eventually(t, func() bool {
		status := coordinatorInstance.StatusResource().Load()
		shard := status.Namespaces["default"].Shards[0]
		return shard.Leader != nil
	}, time.Second, time.Millisecond*100)

	target := sa1.Public
	status := coordinatorInstance.StatusResource().Load()
	shard := status.Namespaces["default"].Shards[0]
	if shard.Leader.GetIdentifier() == sa1.GetIdentifier() {
		target = sa2.Public
	}
	clientPool := clientrpc.NewClientPool(nil, nil)
	defer clientPool.Close()
	clientRpc, err := clientPool.GetClientRpc(target)
	assert.NoError(t, err)
	shardID := int64(0)

	stream, err := clientRpc.Read(t.Context(), &proto.ReadRequest{Shard: &shardID})
	assert.NoError(t, err)
	_, err = stream.Recv()
	assert.NotNil(t, err)
	hint := constant.FindLeaderHint(err)
	assert.NotNil(t, hint)
	assert.Equal(t, shard.Leader.Public, hint.LeaderAddress)
}

func TestLeaderHintWithClient(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	s3, sa3 := mock.NewServer(t, "s3")
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc.NewRpcProvider(clientrpc.NewClientPool(nil, nil)))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	assert.Eventually(t, func() bool {
		status := coordinatorInstance.StatusResource().Load()
		shard := status.Namespaces["default"].Shards[0]
		return shard.Leader != nil
	}, time.Second, time.Millisecond*100)

	target := sa1.Public
	status := coordinatorInstance.StatusResource().Load()
	shard := status.Namespaces["default"].Shards[0]
	if shard.Leader.GetIdentifier() == sa1.GetIdentifier() {
		target = sa2.Public
	}

	client, err := oxia.NewSyncClient(target, oxia.WithNamespace("default"), oxia.WithFailureInjection([]oxia.Failure{oxia.DizzyShardManager}))
	assert.NoError(t, err)

	_, _, err = client.Put(t.Context(), "/key1", []byte("value"))
	assert.NoError(t, err)

	_, _, err = client.Put(t.Context(), "/key2", []byte("value"))
	assert.NoError(t, err)

	key, value, _, err := client.Get(t.Context(), "/key1")
	assert.NoError(t, err)
	assert.Equal(t, "/key1", key)
	assert.Equal(t, []byte("value"), value)

	key, value, _, err = client.Get(t.Context(), "/key2")
	assert.NoError(t, err)
	assert.Equal(t, "/key2", key)
	assert.Equal(t, []byte("value"), value)
}
