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

package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/oxiad/coordinator"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

func NewCoordinator(t *testing.T, config *proto.ClusterConfiguration, clusterConfigNotificationCh chan any) coordinator.Coordinator {
	t.Helper()
	metadataProvider := memory.NewProvider()
	metadata := coordmetadata.New(
		t.Context(),
		metadataProvider,
		coordmetadata.NewClusterConfigProviderFromLoader(
			t.Context(),
			func() (*proto.ClusterConfiguration, error) { return config, nil },
			clusterConfigNotificationCh,
		),
	)
	t.Cleanup(func() {
		assert.NoError(t, metadata.Close())
	})
	coordinatorInstance, err := coordinator.NewCoordinator(metadata, rpc2.NewRpcProviderFactory(nil))
	assert.NoError(t, err)
	return coordinatorInstance
}
