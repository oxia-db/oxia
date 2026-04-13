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

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
)

func TestAdminServerListDataServers(t *testing.T) {
	serverName := "server-1"
	meta := metadata.NewMetadataProviderMemory()
	_, err := meta.Store(&model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"default": {
				Shards: map[int64]model.ShardMetadata{
					1: {
						Ensemble: []model.Server{
							{Name: &serverName, Public: "public-1", Internal: "internal-1"},
						},
					},
				},
			},
		},
	}, metadata.NotExists)
	require.NoError(t, err)
	statusResource := resource.NewStatusResource(meta)

	admin := newAdminServer(
		statusResource,
		func() (model.ClusterConfig, error) {
			return model.ClusterConfig{
				Servers: []model.Server{
					{Name: &serverName, Public: "public-1", Internal: "internal-1"},
				},
				ServerMetadata: map[string]model.ServerMetadata{
					serverName: {Labels: map[string]string{"rack": "a1"}},
				},
			}, nil
		},
		func() map[string]DataServerInfo {
			return map[string]DataServerInfo{
				serverName: {
					Status:            controller.Running,
					FeaturesSupported: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
				},
			}
		},
		nil,
	)

	res, err := admin.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	require.Len(t, res.DataServers, 1)

	dataServer := res.DataServers[0]
	assert.Equal(t, &serverName, dataServer.Name)
	assert.Equal(t, "public-1", dataServer.PublicAddress)
	assert.Equal(t, "internal-1", dataServer.InternalAddress)
	assert.Equal(t, map[string]string{"rack": "a1"}, dataServer.Metadata)
	assert.Equal(t, proto.DataServerStatus_DATA_SERVER_STATUS_RUNNING, dataServer.Status)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, dataServer.FeaturesSupported)
	assert.EqualValues(t, 1, dataServer.ShardCount)
}

func TestAdminServerListDataServersUnknownRuntimeStatus(t *testing.T) {
	serverName := "server-1"
	meta := metadata.NewMetadataProviderMemory()
	_, err := meta.Store(&model.ClusterStatus{}, metadata.NotExists)
	require.NoError(t, err)
	statusResource := resource.NewStatusResource(meta)

	admin := newAdminServer(
		statusResource,
		func() (model.ClusterConfig, error) {
			return model.ClusterConfig{
				Servers: []model.Server{
					{Name: &serverName, Public: "public-1", Internal: "internal-1"},
				},
			}, nil
		},
		nil,
		nil,
	)

	res, err := admin.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	require.Len(t, res.DataServers, 1)
	assert.Equal(t, proto.DataServerStatus_DATA_SERVER_STATUS_UNKNOWN, res.DataServers[0].Status)
	assert.Empty(t, res.DataServers[0].FeaturesSupported)
	assert.Zero(t, res.DataServers[0].ShardCount)
}
