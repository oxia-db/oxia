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

package assignments

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	coordreconciler "github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/tests/mock"
)

func newCoordinatorInstance(
	t *testing.T,
	metadataProvider provider.Provider[*proto.ClusterStatus],
	configProvider provider.Provider[*proto.ClusterConfiguration],
	rpcProvider rpc.ProviderFactory,
) coordruntime.Runtime {
	t.Helper()

	metadataFactory, metadata := mock.NewMetadataFromProviders(t, metadataProvider, configProvider)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, metadataFactory.Close())
	})

	coordinatorInstance, err := coordruntime.New(metadata, rpcProvider)
	require.NoError(t, err)
	reconciler := coordreconciler.New(t.Context(), coordinatorInstance)
	t.Cleanup(func() {
		require.NoError(t, reconciler.Close())
	})
	return coordinatorInstance
}

func newDefaultClusterConfig(servers ...*proto.DataServerIdentity) *proto.ClusterConfiguration {
	return &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: servers,
	}
}
