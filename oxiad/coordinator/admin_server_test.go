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
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

func dataServer(name *string, public, internal string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{
		Name:     name,
		Public:   public,
		Internal: internal,
	}
}

func newTestMetadata(t *testing.T, config *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	metadata := coordmetadata.New(
		t.Context(),
		memory.NewProvider[*proto.ClusterStatus](),
		func() (*proto.ClusterConfiguration, error) {
			return config, nil
		},
		nil,
	)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})
	return metadata
}

func TestAdminServerListDataServers(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"

	admin := newAdminServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(&serverName2, "public-2", "internal-2"),
				dataServer(nil, "public-3", "internal-3"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
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

	require.NotNil(t, res.DataServers[0].Identity)
	require.NotNil(t, res.DataServers[0].Identity.Name)
	assert.Equal(t, serverName1, *res.DataServers[0].Identity.Name)
	assert.Equal(t, "public-1", res.DataServers[0].Identity.GetPublic())
	assert.Equal(t, "internal-1", res.DataServers[0].Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, res.DataServers[0].Metadata.GetLabels())

	require.NotNil(t, res.DataServers[1].Identity)
	require.NotNil(t, res.DataServers[1].Identity.Name)
	assert.Equal(t, serverName2, *res.DataServers[1].Identity.Name)
	assert.Equal(t, "public-2", res.DataServers[1].Identity.GetPublic())
	assert.Equal(t, "internal-2", res.DataServers[1].Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, res.DataServers[1].Metadata.GetLabels())

	require.NotNil(t, res.DataServers[2].Identity)
	require.NotNil(t, res.DataServers[2].Identity.Name)
	assert.Equal(t, "internal-3", *res.DataServers[2].Identity.Name)
	assert.Equal(t, "public-3", res.DataServers[2].Identity.GetPublic())
	assert.Equal(t, "internal-3", res.DataServers[2].Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-3"}, res.DataServers[2].Metadata.GetLabels())
}

func TestAdminServerListNodesUsesInternalAddressWhenNameIsUnset(t *testing.T) {
	serverName1 := "server-1"

	admin := newAdminServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(nil, "public-2", "internal-2"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
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
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-2", "internal-2"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName: {Labels: map[string]string{"zone": "zone-2"}},
			},
		}),
		nil,
	)

	res, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: serverName})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, serverName, *res.DataServer.Identity.Name)
	assert.Equal(t, "public-2", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-2", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"zone": "zone-2"}, res.DataServer.Metadata.GetLabels())
}

func TestAdminServerGetDataServerByIdentifierFallback(t *testing.T) {
	serverName := "server-2"

	admin := newAdminServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-2", "internal-2"),
				dataServer(nil, "public-3", "internal-3"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName:   {Labels: map[string]string{"role": "named"}},
				"internal-3": {Labels: map[string]string{"role": "fallback"}},
			},
		}),
		nil,
	)

	res, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-3"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, "internal-3", *res.DataServer.Identity.Name)
	assert.Equal(t, "public-3", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-3", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"role": "fallback"}, res.DataServer.Metadata.GetLabels())

	_, err = admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-2"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestAdminServerGetDataServerNotFound(t *testing.T) {
	admin := newAdminServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(nil, "public-1", "internal-1"),
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
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := admin.GetDataServer(context.Background(), &proto.GetDataServerRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}
