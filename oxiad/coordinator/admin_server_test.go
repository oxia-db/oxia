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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type testDataServerController struct {
	dataServer model.Server
	features   []proto.Feature
}

func (*testDataServerController) Close() error {
	return nil
}

func (t *testDataServerController) DataServer() model.Server {
	return t.dataServer
}

func (*testDataServerController) Status() controller.DataServerStatus {
	return controller.Running
}

func (t *testDataServerController) SupportedFeatures() []proto.Feature {
	return t.features
}

func (*testDataServerController) SetStatus(controller.DataServerStatus) {
}

type testDataServerFeatureProvider map[string]controller.DataServerController

func (t testDataServerFeatureProvider) NodeControllers() map[string]controller.DataServerController {
	return t
}

type testClusterConfigStore struct {
	config model.ClusterConfig
}

func (t *testClusterConfigStore) Update(mutator func(*model.ClusterConfig) error) (model.ClusterConfig, error) {
	config := t.config
	if err := mutator(&config); err != nil {
		return model.ClusterConfig{}, err
	}

	t.config = config
	return config, nil
}

func TestAdminServerListDataServers(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"
	serverName3 := "server-3"

	admin := newAdminServer(
		nil,
		func() (model.ClusterConfig, error) {
			return model.ClusterConfig{
				Servers: []model.Server{
					{Name: &serverName1, Public: "public-1", Internal: "internal-1"},
					{Name: &serverName2, Public: "public-2", Internal: "internal-2"},
					{Name: &serverName3, Public: "public-3", Internal: "internal-3"},
				},
				ServerMetadata: map[string]model.ServerMetadata{
					serverName2: {Labels: map[string]string{"rack": "r2"}},
				},
			}, nil
		},
		nil,
		nil,
		testDataServerFeatureProvider{
			serverName2: &testDataServerController{features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
		},
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
	assert.Equal(t, map[string]string{"rack": "r2"}, res.DataServers[1].Metadata)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, res.DataServers[1].SupportedFeatures)

	require.NotNil(t, res.DataServers[2].Name)
	assert.Equal(t, serverName3, *res.DataServers[2].Name)
	assert.Equal(t, "public-3", res.DataServers[2].PublicAddress)
	assert.Equal(t, "internal-3", res.DataServers[2].InternalAddress)
	assert.Nil(t, res.DataServers[2].Metadata)
	assert.Nil(t, res.DataServers[2].SupportedFeatures)
}

func TestAdminServerGetDataServer(t *testing.T) {
	serverName := "server-2"

	admin := newAdminServer(
		nil,
		func() (model.ClusterConfig, error) {
			return model.ClusterConfig{
				Servers: []model.Server{
					{Public: "public-1", Internal: "internal-1"},
					{Name: &serverName, Public: "public-2", Internal: "internal-2"},
				},
				ServerMetadata: map[string]model.ServerMetadata{
					serverName: {Labels: map[string]string{"zone": "z1"}},
				},
			}, nil
		},
		nil,
		nil,
		testDataServerFeatureProvider{
			serverName: &testDataServerController{features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
		},
	)

	res, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: serverName})
	require.NoError(t, err)
	require.NotNil(t, res.Name)
	assert.Equal(t, serverName, *res.Name)
	assert.Equal(t, "public-2", res.PublicAddress)
	assert.Equal(t, "internal-2", res.InternalAddress)
	assert.Equal(t, map[string]string{"zone": "z1"}, res.Metadata)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, res.SupportedFeatures)
}

func TestAdminServerGetDataServerFallsBackToInternalAddressWhenNameIsEmpty(t *testing.T) {
	emptyName := ""
	internalAddress := "internal-1"

	admin := newAdminServer(
		nil,
		func() (model.ClusterConfig, error) {
			return model.ClusterConfig{
				Servers: []model.Server{
					{Name: &emptyName, Public: "public-1", Internal: internalAddress},
				},
				ServerMetadata: map[string]model.ServerMetadata{
					internalAddress: {Labels: map[string]string{"zone": "z1"}},
				},
			}, nil
		},
		nil,
		nil,
		testDataServerFeatureProvider{
			internalAddress: &testDataServerController{features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
		},
	)

	res, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: internalAddress})
	require.NoError(t, err)
	require.NotNil(t, res.Name)
	assert.Equal(t, emptyName, *res.Name)
	assert.Equal(t, "public-1", res.PublicAddress)
	assert.Equal(t, internalAddress, res.InternalAddress)
	assert.Equal(t, map[string]string{"zone": "z1"}, res.Metadata)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, res.SupportedFeatures)
}

func TestAdminServerPatchDataServer(t *testing.T) {
	serverName := "server-1"
	publicAddress := "public-2"
	internalAddress := "internal-2"

	store := &testClusterConfigStore{
		config: model.ClusterConfig{
			Servers: []model.Server{
				{Name: &serverName, Public: "public-1", Internal: "internal-1"},
			},
			ServerMetadata: map[string]model.ServerMetadata{
				serverName: {Labels: map[string]string{"rack": "r1"}},
			},
		},
	}

	admin := newAdminServer(
		nil,
		func() (model.ClusterConfig, error) {
			return store.config, nil
		},
		store,
		nil,
		testDataServerFeatureProvider{
			serverName: &testDataServerController{features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
		},
	)

	res, err := admin.PatchDataServer(context.Background(), &proto.PatchDataServerRequest{
		DataServer:      serverName,
		PublicAddress:   &publicAddress,
		InternalAddress: &internalAddress,
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "r2"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res.Name)
	assert.Equal(t, serverName, *res.Name)
	assert.Equal(t, publicAddress, res.PublicAddress)
	assert.Equal(t, internalAddress, res.InternalAddress)
	assert.Equal(t, map[string]string{"rack": "r2"}, res.Metadata)
	assert.Equal(t, publicAddress, store.config.Servers[0].Public)
	assert.Equal(t, internalAddress, store.config.Servers[0].Internal)
}

func TestAdminServerPatchDataServerRejectsUnnamedInternalAddress(t *testing.T) {
	store := &testClusterConfigStore{
		config: model.ClusterConfig{
			Servers: []model.Server{
				{Public: "public-1", Internal: "internal-1"},
			},
		},
	}

	internalAddress := "internal-2"
	admin := newAdminServer(
		nil,
		func() (model.ClusterConfig, error) {
			return store.config, nil
		},
		store,
		nil,
		nil,
	)

	_, err := admin.PatchDataServer(context.Background(), &proto.PatchDataServerRequest{
		DataServer:      "internal-1",
		InternalAddress: &internalAddress,
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestAdminServerGetDataServerNotFound(t *testing.T) {
	admin := newAdminServer(
		nil,
		func() (model.ClusterConfig, error) {
			return model.ClusterConfig{
				Servers: []model.Server{
					{Public: "public-1", Internal: "internal-1"},
				},
			}, nil
		},
		nil,
		nil,
		nil,
	)

	_, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}
