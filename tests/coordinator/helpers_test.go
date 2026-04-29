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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/tests/mock"
)

func createCoordinatorMetadata(
	t *testing.T,
	metadataProvider provider.Provider[*proto.ClusterStatus],
	clusterConfigProvider func() (*proto.ClusterConfiguration, error),
	clusterConfigNotificationsCh chan any,
) coordmetadata.Metadata {
	t.Helper()

	configProvider := mock.NewConfigProvider(t, clusterConfigProvider, clusterConfigNotificationsCh)
	metadataFactory := coordmetadata.NewFactoryWithProviders(metadataProvider, configProvider)
	t.Cleanup(func() {
		require.NoError(t, metadataFactory.Close())
	})

	metadata, err := metadataFactory.CreateMetadata(t.Context())
	require.NoError(t, err)
	return metadata
}

func newCoordinatorInstance(
	t *testing.T,
	metadataProvider provider.Provider[*proto.ClusterStatus],
	clusterConfigProvider func() (*proto.ClusterConfiguration, error),
	clusterConfigNotificationsCh chan any,
	rpcProvider rpc2.ProviderFactory,
) coordruntime.Runtime {
	t.Helper()

	metadata := createCoordinatorMetadata(t, metadataProvider, clusterConfigProvider, clusterConfigNotificationsCh)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})

	coordinatorInstance, err := coordruntime.New(metadata, rpcProvider)
	require.NoError(t, err)
	return coordinatorInstance
}

func newClusterConfig(namespaces []*proto.Namespace, servers []*proto.DataServerIdentity) *proto.ClusterConfiguration {
	return &proto.ClusterConfiguration{
		Namespaces: namespaces,
		Servers:    servers,
	}
}
