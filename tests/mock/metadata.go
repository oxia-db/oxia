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

package mock

import (
	"testing"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

func NewConfigProvider(t *testing.T, clusterConfig *proto.ClusterConfiguration) provider.Provider[*proto.ClusterConfiguration] {
	t.Helper()

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	PutConfig(t, configProvider, clusterConfig)
	return configProvider
}

func PutConfig(
	t *testing.T,
	configProvider provider.Provider[*proto.ClusterConfiguration],
	clusterConfig *proto.ClusterConfiguration,
) {
	t.Helper()

	version := configProvider.Watch().Load().Version
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: version,
	})
	require.NoError(t, err)
}
