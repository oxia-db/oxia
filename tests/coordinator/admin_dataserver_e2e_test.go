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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordserver "github.com/oxia-db/oxia/oxiad/coordinator"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestAdminDataServerCRUD(t *testing.T) {
	metadataDir := t.TempDir()

	options := coordoption.NewDefaultOptions()
	options.Server.Internal.BindAddress = "127.0.0.1:0"
	adminAddr := mock.FreeAddresses(t, 1)[0]
	options.Server.Public.BindAddress = adminAddr
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.ProviderName = metadatacommon.NameFile
	options.Metadata.File.Dir = metadataDir
	options.Metadata.File.ConfigName = "cluster.yaml"

	coordinatorServer, err := coordserver.New(t.Context(), commonwatch.New(options))
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
	created, err := client.CreateDataServer(t.Context(), &proto.DataServer{
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
	_, err = client.CreateDataServer(t.Context(), &proto.DataServer{
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

	dataServers, err := client.ListDataServers(t.Context())
	require.NoError(t, err)
	require.Len(t, dataServers, 2)

	found, err := client.GetDataServer(t.Context(), serverName)
	require.NoError(t, err)
	assert.Equal(t, serverName, found.GetDataServer().GetIdentity().GetName())
	assert.Equal(t, "localhost:19001", found.GetDataServer().GetIdentity().GetPublic())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, found.GetDataServer().GetMetadata().GetLabels())

	patched, err := client.PatchDataServer(t.Context(), &proto.DataServer{
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

	// The reconciler creates the controller of server-1 in the background: only
	// a data server that joined is drained, and reported, once it is deleted
	require.Eventually(t, func() bool {
		view, err := client.GetDataServer(t.Context(), serverName)
		return err == nil && view.GetDataServerStatus().GetState() == proto.DataServerState_DATA_SERVER_STATE_RUNNING
	}, 10*time.Second, 10*time.Millisecond)

	deleted, err := client.DeleteDataServer(t.Context(), serverName)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.GetIdentity())
	assert.Equal(t, serverName, deleted.GetIdentity().GetName())
	assert.Equal(t, "localhost:19101", deleted.GetIdentity().GetPublic())
	assert.Equal(t, "localhost:19002", deleted.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-3"}, deleted.GetMetadata().GetLabels())

	// The deleted data server is reported, by name, once the reconciler starts
	// draining it, until it is forgotten
	require.Eventually(t, func() bool {
		dataServers, err = client.ListDataServers(t.Context())
		return err == nil && len(dataServers) == 2
	}, 10*time.Second, 10*time.Millisecond)
	require.NotNil(t, dataServers[0].GetDataServer().GetIdentity())
	assert.Equal(t, remainingServerName, dataServers[0].GetDataServer().GetIdentity().GetName())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, dataServers[0].GetDataServer().GetMetadata().GetLabels())
	assert.Equal(t, serverName, dataServers[1].GetDataServer().GetIdentity().GetName())
	assert.Contains(t, []proto.DataServerState{proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		proto.DataServerState_DATA_SERVER_STATE_DRAINED}, dataServers[1].GetDataServerStatus().GetState())

	// A retried delete returns the data server that left
	deleted, err = client.DeleteDataServer(t.Context(), serverName)
	require.NoError(t, err)
	assert.Equal(t, serverName, deleted.GetIdentity().GetName())

	require.Eventually(t, func() bool {
		view, err := client.GetDataServer(t.Context(), serverName)
		return err == nil && view.GetDataServerStatus().GetState() == proto.DataServerState_DATA_SERVER_STATE_DRAINED
	}, 10*time.Second, 10*time.Millisecond)

	// The confirmed drain forgets the data server; a retried confirm succeeds
	require.NoError(t, client.ConfirmDrainedDataServer(t.Context(), serverName))
	require.Eventually(t, func() bool {
		_, err := client.GetDataServer(t.Context(), serverName)
		return grpcstatus.Code(err) == codes.NotFound
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, client.ConfirmDrainedDataServer(t.Context(), serverName))
	_, err = client.DeleteDataServer(t.Context(), serverName)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}
