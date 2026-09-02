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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func newMemoryFactory(t *testing.T) *Factory {
	t.Helper()

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameMemory,
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, factory.Close())
	})
	return factory
}

func TestSeedClusterConfig(t *testing.T) {
	factory := newMemoryFactory(t)

	config := &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              "test-namespace",
			ReplicationFactor: 1,
			InitialShardCount: 1,
		}},
	}
	require.NoError(t, factory.SeedClusterConfig(config))

	seeded := factory.configProvider.Watch().Load()
	assert.NotEqual(t, metadatacommon.NotExists, seeded.Version)
	require.Len(t, seeded.Value.Namespaces, 1)
	assert.Equal(t, "test-namespace", seeded.Value.Namespaces[0].Name)
}

func TestSeedClusterConfigRejectsNil(t *testing.T) {
	factory := newMemoryFactory(t)

	require.Error(t, factory.SeedClusterConfig(nil))
	assert.Equal(t, metadatacommon.NotExists, factory.configProvider.Watch().Load().Version)
}

func TestSeedClusterConfigDoesNotOverwrite(t *testing.T) {
	factory := newMemoryFactory(t)

	first := &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{Name: "first"}},
	}
	require.NoError(t, factory.SeedClusterConfig(first))
	version := factory.configProvider.Watch().Load().Version

	second := &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{Name: "second"}},
	}
	require.NoError(t, factory.SeedClusterConfig(second))

	current := factory.configProvider.Watch().Load()
	assert.Equal(t, version, current.Version)
	require.Len(t, current.Value.Namespaces, 1)
	assert.Equal(t, "first", current.Value.Namespaces[0].Name)
}
