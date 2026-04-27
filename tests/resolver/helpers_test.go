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

package resolver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	coordinatorrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

func newCoordinatorInstance(
	t *testing.T,
	metadataProvider provider.StatusProvider,
	clusterConfigProvider func() (*proto.ClusterConfiguration, error),
	clusterConfigNotificationsCh chan any,
	rpcProvider coordinatorrpc.ProviderFactory,
) coordinator.Coordinator {
	t.Helper()

	metadata := coordmetadata.New(t.Context(), metadataProvider, clusterConfigProvider, clusterConfigNotificationsCh)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})

	coordinatorInstance, err := coordinator.NewCoordinator(metadata, rpcProvider)
	require.NoError(t, err)
	return coordinatorInstance
}
