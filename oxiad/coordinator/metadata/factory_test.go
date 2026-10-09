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

func newSeedConfig(namespace string) *commonproto.ClusterConfiguration {
	return &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              namespace,
			ReplicationFactor: 1,
			InitialShardCount: 1,
		}},
		Servers: []*commonproto.DataServerIdentity{{
			Public:   "localhost:6648",
			Internal: "localhost:6649",
		}},
	}
}

func loadConfig(t *testing.T, factory *Factory) *commonproto.ClusterConfiguration {
	t.Helper()

	current, err := factory.configProvider.Load()
	require.NoError(t, err)
	if current.Version == metadatacommon.NotExists {
		return nil
	}
	return current.Value
}

func TestSeedClusterConfig(t *testing.T) {
	factory := newMemoryFactory(t)

	require.NoError(t, factory.SeedClusterConfig(newSeedConfig("test-namespace")))

	seeded := loadConfig(t, factory)
	require.NotNil(t, seeded)
	require.Len(t, seeded.Namespaces, 1)
	assert.Equal(t, "test-namespace", seeded.Namespaces[0].Name)
}

func TestSeedClusterConfigDoesNotOverwrite(t *testing.T) {
	factory := newMemoryFactory(t)

	require.NoError(t, factory.SeedClusterConfig(newSeedConfig("first")))
	require.NoError(t, factory.SeedClusterConfig(newSeedConfig("second")))

	current := loadConfig(t, factory)
	require.NotNil(t, current)
	require.Len(t, current.Namespaces, 1)
	assert.Equal(t, "first", current.Namespaces[0].Name)
}

// An invalid configuration is rejected before anything is stored: once stored,
// it would never be replaced by a later seed, and a coordinator would fail on
// it at every start.
func TestSeedClusterConfigRejectsInvalid(t *testing.T) {
	replicationAboveServers := newSeedConfig("test-namespace")
	replicationAboveServers.Namespaces[0].ReplicationFactor = 3

	noShards := newSeedConfig("test-namespace")
	noShards.Namespaces[0].InitialShardCount = 0

	for name, config := range map[string]*commonproto.ClusterConfiguration{
		"nil":                         nil,
		"replication above servers":   replicationAboveServers,
		"no initial shards":           noShards,
		"coordinator without address": {Coordinators: []*commonproto.Coordinator{{Name: "c1"}}},
	} {
		t.Run(name, func(t *testing.T) {
			factory := newMemoryFactory(t)

			require.Error(t, factory.SeedClusterConfig(config))
			assert.Nil(t, loadConfig(t, factory))
		})
	}
}
