// Copyright 2025 StreamNative, Inc.
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

	"github.com/streamnative/oxia/common/rpc"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
)

func NewCoordinator(t *testing.T, config *model.ClusterConfig, clusterConfigNotificationCh chan any) impl.Coordinator {
	t.Helper()
	metadataProvider := impl.NewMetadataProviderMemory()
	clientPool := rpc.NewClientPool(nil, nil)
	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return *config, nil }, clusterConfigNotificationCh, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	return coordinator
}
