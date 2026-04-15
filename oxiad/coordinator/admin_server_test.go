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

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
)

func newTestClusterConfigResource(t *testing.T, config model.ClusterConfig) resource.ClusterConfigResource {
	t.Helper()

	configResource := resource.NewClusterConfigResource(
		t.Context(),
		func() (model.ClusterConfig, error) {
			return config, nil
		},
		nil,
		nil,
	)
	t.Cleanup(func() {
		require.NoError(t, configResource.Close())
	})
	return configResource
}

type inMemoryClusterConfigManager struct {
	clusterConfig                model.ClusterConfig
	clusterConfigNotificationsCh chan any
}

func (m *inMemoryClusterConfigManager) Load() (model.ClusterConfig, error) {
	return cloneClusterConfig(m.clusterConfig), nil
}

func (m *inMemoryClusterConfigManager) Update(mutator func(*model.ClusterConfig) error) (model.ClusterConfig, error) {
	clusterConfig := cloneClusterConfig(m.clusterConfig)
	if err := mutator(&clusterConfig); err != nil {
		return model.ClusterConfig{}, err
	}
	if err := clusterConfig.Validate(); err != nil {
		return model.ClusterConfig{}, err
	}
	m.clusterConfig = cloneClusterConfig(clusterConfig)
	if m.clusterConfigNotificationsCh != nil {
		m.clusterConfigNotificationsCh <- struct{}{}
	}
	return cloneClusterConfig(m.clusterConfig), nil
}

type noopClusterConfigEventListener struct{}

func (noopClusterConfigEventListener) ConfigChanged(*model.ClusterConfig) {}

func cloneClusterConfig(config model.ClusterConfig) model.ClusterConfig {
	cloned := config
	cloned.Namespaces = append([]model.NamespaceConfig(nil), config.Namespaces...)
	cloned.Servers = append([]model.Server(nil), config.Servers...)
	cloned.AllowExtraAuthorities = append([]string(nil), config.AllowExtraAuthorities...)
	if config.ServerMetadata != nil {
		cloned.ServerMetadata = make(map[string]model.ServerMetadata, len(config.ServerMetadata))
		for key, metadata := range config.ServerMetadata {
			cloned.ServerMetadata[key] = model.ServerMetadata{
				Labels: cloneLabels(metadata.Labels),
			}
		}
	}
	if config.LoadBalancer != nil {
		loadBalancer := *config.LoadBalancer
		cloned.LoadBalancer = &loadBalancer
	}
	return cloned
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func newWritableTestClusterConfigResource(
	t *testing.T,
	manager *inMemoryClusterConfigManager,
	clusterConfigNotificationsCh chan any,
	listener resource.ClusterConfigEventListener,
) resource.ClusterConfigResource {
	t.Helper()

	configResource := resource.NewClusterConfigResource(
		t.Context(),
		manager.Load,
		clusterConfigNotificationsCh,
		listener,
		resource.WithClusterConfigUpdater(manager.Update),
	)
	t.Cleanup(func() {
		require.NoError(t, configResource.Close())
	})
	return configResource
}

func TestAdminServerListDataServers(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"

	admin := newAdminServer(
		nil,
		newTestClusterConfigResource(t, model.ClusterConfig{
			Servers: []model.Server{
				{Name: &serverName1, Public: "public-1", Internal: "internal-1"},
				{Name: &serverName2, Public: "public-2", Internal: "internal-2"},
				{Public: "public-3", Internal: "internal-3"},
			},
			ServerMetadata: map[string]model.ServerMetadata{
				serverName1:  {Labels: map[string]string{"rack": "rack-1"}},
				serverName2:  {Labels: map[string]string{"rack": "rack-2"}},
				"internal-3": {Labels: map[string]string{"rack": "rack-3"}},
			},
		}),
		nil,
	)

	res, err := admin.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	require.Len(t, res.DataServers, 3)

	require.NotNil(t, res.DataServers[0].Name)
	assert.Equal(t, serverName1, *res.DataServers[0].Name)
	assert.Equal(t, "public-1", res.DataServers[0].PublicAddress)
	assert.Equal(t, "internal-1", res.DataServers[0].InternalAddress)

	require.NotNil(t, res.DataServers[1].Name)
	assert.Equal(t, serverName2, *res.DataServers[1].Name)
	assert.Equal(t, "public-2", res.DataServers[1].PublicAddress)
	assert.Equal(t, "internal-2", res.DataServers[1].InternalAddress)

	require.NotNil(t, res.DataServers[2].Name)
	assert.Equal(t, "internal-3", *res.DataServers[2].Name)
	assert.Equal(t, "public-3", res.DataServers[2].PublicAddress)
	assert.Equal(t, "internal-3", res.DataServers[2].InternalAddress)
}

func TestAdminServerListNodesUsesInternalAddressWhenNameIsUnset(t *testing.T) {
	serverName1 := "server-1"

	admin := newAdminServer(
		nil,
		newTestClusterConfigResource(t, model.ClusterConfig{
			Servers: []model.Server{
				{Name: &serverName1, Public: "public-1", Internal: "internal-1"},
				{Public: "public-2", Internal: "internal-2"},
			},
			ServerMetadata: map[string]model.ServerMetadata{
				serverName1:  {Labels: map[string]string{"role": "named"}},
				"internal-2": {Labels: map[string]string{"role": "fallback"}},
			},
		}),
		nil,
	)

	res, err := admin.ListNodes(context.Background(), &proto.ListNodesRequest{})
	require.NoError(t, err)
	require.Len(t, res.Nodes, 2)

	require.NotNil(t, res.Nodes[0].Name)
	assert.Equal(t, serverName1, *res.Nodes[0].Name)
	assert.Equal(t, map[string]string{"role": "named"}, res.Nodes[0].Metadata)

	require.NotNil(t, res.Nodes[1].Name)
	assert.Equal(t, "internal-2", *res.Nodes[1].Name)
	assert.Equal(t, "public-2", res.Nodes[1].PublicAddress)
	assert.Equal(t, "internal-2", res.Nodes[1].InternalAddress)
	assert.Equal(t, map[string]string{"role": "fallback"}, res.Nodes[1].Metadata)
}

func TestAdminServerGetDataServerByName(t *testing.T) {
	serverName := "server-2"

	admin := newAdminServer(
		nil,
		newTestClusterConfigResource(t, model.ClusterConfig{
			Servers: []model.Server{
				{Name: &serverName, Public: "public-2", Internal: "internal-2"},
			},
			ServerMetadata: map[string]model.ServerMetadata{
				serverName: {Labels: map[string]string{"zone": "zone-2"}},
			},
		}),
		nil,
	)

	res, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: serverName})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServerInfo)
	require.NotNil(t, res.DataServerInfo.DataServer)
	require.NotNil(t, res.DataServerInfo.DataServer.Name)
	assert.Equal(t, serverName, *res.DataServerInfo.DataServer.Name)
	assert.Equal(t, "public-2", res.DataServerInfo.DataServer.PublicAddress)
	assert.Equal(t, "internal-2", res.DataServerInfo.DataServer.InternalAddress)
	assert.Equal(t, map[string]string{"zone": "zone-2"}, res.DataServerInfo.Metadata)
}

func TestAdminServerCreateDataServerStoresMetadata(t *testing.T) {
	existingName := "server-1"
	manager := &inMemoryClusterConfigManager{
		clusterConfig: model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:              "default",
				ReplicationFactor: 1,
				InitialShardCount: 1,
			}},
			Servers: []model.Server{
				{Name: &existingName, Public: "public-1:6648", Internal: "internal-1:6649"},
			},
		},
	}

	admin := newAdminServer(nil, newWritableTestClusterConfigResource(t, manager, nil, nil), nil)

	newName := "server-2"
	res, err := admin.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
		DataServerInfo: &proto.DataServerInfo{
			DataServer: &proto.DataServer{
				Name:            &newName,
				PublicAddress:   "public-2:6648",
				InternalAddress: "internal-2:6649",
			},
			Metadata: map[string]string{
				"rack": "a",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServerInfo)
	require.NotNil(t, res.DataServerInfo.DataServer)
	require.NotNil(t, res.DataServerInfo.DataServer.Name)
	assert.Equal(t, newName, *res.DataServerInfo.DataServer.Name)
	assert.Equal(t, "public-2:6648", res.DataServerInfo.DataServer.PublicAddress)
	assert.Equal(t, "internal-2:6649", res.DataServerInfo.DataServer.InternalAddress)
	assert.Equal(t, map[string]string{"rack": "a"}, res.DataServerInfo.Metadata)

	require.Len(t, manager.clusterConfig.Servers, 2)
	assert.Equal(t, "public-2:6648", manager.clusterConfig.Servers[1].Public)
	assert.Equal(t, "internal-2:6649", manager.clusterConfig.Servers[1].Internal)
	assert.Equal(t, "a", manager.clusterConfig.ServerMetadata[newName].Labels["rack"])
}

func TestAdminServerCreateDataServerRejectsDuplicateIdentifier(t *testing.T) {
	existingName := "server-1"
	manager := &inMemoryClusterConfigManager{
		clusterConfig: model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:              "default",
				ReplicationFactor: 1,
				InitialShardCount: 1,
			}},
			Servers: []model.Server{
				{Name: &existingName, Public: "public-1:6648", Internal: "internal-1:6649"},
			},
		},
	}

	admin := newAdminServer(nil, newWritableTestClusterConfigResource(t, manager, nil, nil), nil)

	_, err := admin.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
		DataServerInfo: &proto.DataServerInfo{
			DataServer: &proto.DataServer{
				Name:            &existingName,
				PublicAddress:   "public-2:6648",
				InternalAddress: "internal-2:6649",
			},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, grpcstatus.Code(err))
}

func TestAdminServerCreateDataServerNotifiesClusterConfigResource(t *testing.T) {
	clusterConfigNotificationsCh := make(chan any, 1)
	manager := &inMemoryClusterConfigManager{
		clusterConfig: model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:              "default",
				ReplicationFactor: 1,
				InitialShardCount: 1,
			}},
			Servers: []model.Server{{
				Public:   "public-1:6648",
				Internal: "internal-1:6649",
			}},
		},
		clusterConfigNotificationsCh: clusterConfigNotificationsCh,
	}

	clusterConfigResource := newWritableTestClusterConfigResource(
		t,
		manager,
		clusterConfigNotificationsCh,
		noopClusterConfigEventListener{},
	)

	_, exists := clusterConfigResource.Node("internal-1:6649")
	require.True(t, exists)

	admin := newAdminServer(nil, clusterConfigResource, nil)
	_, err := admin.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
		DataServerInfo: &proto.DataServerInfo{
			DataServer: &proto.DataServer{
				PublicAddress:   "public-2:6648",
				InternalAddress: "internal-2:6649",
			},
			Metadata: map[string]string{
				"rack": "b",
			},
		},
	})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		server, ok := clusterConfigResource.Node("internal-2:6649")
		return ok && server.Public == "public-2:6648"
	}, 5*time.Second, 50*time.Millisecond)
}

func TestAdminServerGetDataServerByIdentifierFallback(t *testing.T) {
	serverName := "server-2"

	admin := newAdminServer(
		nil,
		newTestClusterConfigResource(t, model.ClusterConfig{
			Servers: []model.Server{
				{Name: &serverName, Public: "public-2", Internal: "internal-2"},
				{Public: "public-3", Internal: "internal-3"},
			},
			ServerMetadata: map[string]model.ServerMetadata{
				serverName:   {Labels: map[string]string{"role": "named"}},
				"internal-3": {Labels: map[string]string{"role": "fallback"}},
			},
		}),
		nil,
	)

	res, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-3"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServerInfo)
	require.NotNil(t, res.DataServerInfo.DataServer)
	require.NotNil(t, res.DataServerInfo.DataServer.Name)
	assert.Equal(t, "internal-3", *res.DataServerInfo.DataServer.Name)
	assert.Equal(t, "public-3", res.DataServerInfo.DataServer.PublicAddress)
	assert.Equal(t, "internal-3", res.DataServerInfo.DataServer.InternalAddress)
	assert.Equal(t, map[string]string{"role": "fallback"}, res.DataServerInfo.Metadata)

	_, err = admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-2"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestAdminServerGetDataServerNotFound(t *testing.T) {
	admin := newAdminServer(
		nil,
		newTestClusterConfigResource(t, model.ClusterConfig{
			Servers: []model.Server{
				{Public: "public-1", Internal: "internal-1"},
			},
		}),
		nil,
	)

	_, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestAdminServerGetDataServerRejectsEmptyLookup(t *testing.T) {
	admin := newAdminServer(
		nil,
		newTestClusterConfigResource(t, model.ClusterConfig{}),
		nil,
	)

	_, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}
