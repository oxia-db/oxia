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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordserver "github.com/oxia-db/oxia/oxiad/coordinator"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestAdminDataServerCRUD(t *testing.T) {
	metadataDir := t.TempDir()

	adminAddr := freeAddress(t)
	options := coordoption.NewDefaultOptions()
	options.Server.Internal.BindAddress = "127.0.0.1:0"
	options.Server.Admin.BindAddress = adminAddr
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.ProviderName = metadatacommon.NameFile
	options.Metadata.File.Dir = metadataDir
	options.Metadata.File.ConfigName = "cluster.yaml"

	coordinatorServer, err := coordserver.NewGrpcServer(t.Context(), commonwatch.New(options))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, coordinatorServer.Close())
	})

	client, err := oxia.NewAdminClient(adminAddr, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	serverName := "server-1"
	created, err := client.CreateDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "localhost:19001",
			Internal: "localhost:19002",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, serverName, created.GetIdentity().GetName())
	assert.Equal(t, "localhost:19001", created.GetIdentity().GetPublic())
	assert.Equal(t, "localhost:19002", created.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, created.GetMetadata().GetLabels())

	remainingServerName := "server-2"
	_, err = client.CreateDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &remainingServerName,
			Public:   "localhost:19003",
			Internal: "localhost:19004",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-2"},
		},
	})
	require.NoError(t, err)

	dataServers, err := client.ListDataServers()
	require.NoError(t, err)
	require.Len(t, dataServers, 2)

	found, err := client.GetDataServer(serverName)
	require.NoError(t, err)
	assert.Equal(t, serverName, found.GetIdentity().GetName())
	assert.Equal(t, "localhost:19001", found.GetIdentity().GetPublic())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, found.GetMetadata().GetLabels())

	patched, err := client.PatchDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:   &serverName,
			Public: "localhost:19101",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-3"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, serverName, patched.GetIdentity().GetName())
	assert.Equal(t, "localhost:19101", patched.GetIdentity().GetPublic())
	assert.Equal(t, "localhost:19002", patched.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-3"}, patched.GetMetadata().GetLabels())

	deleted, err := client.DeleteDataServer(serverName)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.GetIdentity())
	assert.Equal(t, serverName, deleted.GetIdentity().GetName())
	assert.Equal(t, "localhost:19101", deleted.GetIdentity().GetPublic())
	assert.Equal(t, "localhost:19002", deleted.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-3"}, deleted.GetMetadata().GetLabels())

	dataServers, err = client.ListDataServers()
	require.NoError(t, err)
	require.Len(t, dataServers, 1)
	require.NotNil(t, dataServers[0].GetIdentity())
	assert.Equal(t, remainingServerName, dataServers[0].GetIdentity().GetName())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, dataServers[0].GetMetadata().GetLabels())
}

func freeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}
