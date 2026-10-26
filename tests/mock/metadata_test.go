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
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
)

func TestNewConfigProviderStoresConfig(t *testing.T) {
	config := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns", ReplicationFactor: 1, InitialShardCount: 1}},
	}

	configProvider := NewConfigProvider(t, config)
	defer configProvider.Close()

	assert.True(t, gproto.Equal(config, Load(t, configProvider).Value))
}

func TestPutConfigReplacesConfig(t *testing.T) {
	configProvider := NewConfigProvider(t, &proto.ClusterConfiguration{})
	defer configProvider.Close()
	updated := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns", ReplicationFactor: 1, InitialShardCount: 1}},
	}

	PutConfig(t, configProvider, updated)

	assert.True(t, gproto.Equal(updated, Load(t, configProvider).Value))
}

func TestMirrorProviderToFileWritesChanges(t *testing.T) {
	configProvider := NewConfigProvider(t, &proto.ClusterConfiguration{})
	path := filepath.Join(t.TempDir(), "config.yaml")
	mirrorProviderToFile(t, path, metadatacodec.ClusterConfigCodec, configProvider)

	updated := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns", ReplicationFactor: 1, InitialShardCount: 1}},
	}
	expected, err := metadatacodec.ClusterConfigCodec.MarshalYAML(updated)
	require.NoError(t, err)
	PutConfig(t, configProvider, updated)

	assert.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		return err == nil && bytes.Equal(expected, data)
	}, 5*time.Second, 10*time.Millisecond)

	// Closing the provider closes the subscription, which ends the mirroring
	require.NoError(t, configProvider.Close())
}
