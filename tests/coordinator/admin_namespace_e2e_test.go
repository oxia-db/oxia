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

func TestAdminNamespaceCreateAndGet(t *testing.T) {
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
	_, err = client.CreateDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "localhost:19001",
			Internal: "localhost:19002",
		},
	})
	require.NoError(t, err)

	notificationsEnabled := false
	created, err := client.CreateNamespace(&proto.Namespace{
		Name:                 "ns-1",
		InitialShardCount:    2,
		ReplicationFactor:    1,
		NotificationsEnabled: &notificationsEnabled,
		KeySorting:           "natural",
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, "ns-1", created.GetName())
	assert.EqualValues(t, 2, created.GetInitialShardCount())
	assert.EqualValues(t, 1, created.GetReplicationFactor())
	assert.False(t, created.NotificationsEnabledOrDefault())
	assert.Equal(t, "natural", created.GetKeySorting())

	found, err := client.GetNamespace("ns-1")
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, "ns-1", found.GetName())
	assert.EqualValues(t, 2, found.GetInitialShardCount())
	assert.False(t, found.NotificationsEnabledOrDefault())

	namespaces, err := client.ListNamespaces()
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	assert.Equal(t, "ns-1", namespaces[0].GetName())
}
