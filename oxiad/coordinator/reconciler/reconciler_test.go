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

package reconciler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

type recordingCoordinator struct {
	configs []*commonproto.ClusterConfiguration
}

func (c *recordingCoordinator) ReconcileClusterConfig(config *commonproto.ClusterConfiguration) error {
	c.configs = append(c.configs, config)
	return nil
}

func TestReconcilerHandlesMetadataStoreEvent(t *testing.T) {
	clusterConfig := &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
	}
	configChangesCh := make(chan any, 1)
	store := coordmetadata.NewProviderClusterConfigStore(t.Context(), func() (*commonproto.ClusterConfiguration, error) {
		return clusterConfig, nil
	}, configChangesCh)
	metadata := coordmetadata.New(t.Context(), memory.NewProvider(), store)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})

	coordinator := &recordingCoordinator{}
	New(t.Context(), metadata, coordinator)
	require.Empty(t, coordinator.configs)

	clusterConfig = &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              "default",
			InitialShardCount: 2,
			ReplicationFactor: 1,
		}},
	}
	configChangesCh <- nil

	require.Eventually(t, func() bool {
		return len(coordinator.configs) == 1 &&
			coordinator.configs[0].GetNamespaces()[0].GetInitialShardCount() == 2
	}, 5*time.Second, 10*time.Millisecond)
}
