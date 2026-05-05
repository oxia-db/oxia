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
	"context"
	"os"
	"path/filepath"
	"testing"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

const metadataFilePerm = 0o600

func NewConfigProvider(t *testing.T, clusterConfig *proto.ClusterConfiguration) provider.Provider[*proto.ClusterConfiguration] {
	t.Helper()

	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled)
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

func NewMetadataFromProviders(
	t *testing.T,
	statusProvider provider.Provider[*proto.ClusterStatus],
	configProvider provider.Provider[*proto.ClusterConfiguration],
) (*coordmetadata.Factory, coordmetadata.Metadata) {
	t.Helper()

	dir := t.TempDir()
	statusPath := filepath.Join(dir, coordoption.DefaultFileStatusName)
	configPath := filepath.Join(dir, coordoption.DefaultFileConfigName)
	writeSnapshot(t, statusPath, metadatacodec.ClusterStatusCodec, statusProvider.Watch().Load().Value)
	writeSnapshot(t, configPath, metadatacodec.ClusterConfigCodec, configProvider.Watch().Load().Value)
	mirrorProviderToFile(t, configPath, metadatacodec.ClusterConfigCodec, configProvider)

	metadataFactory, err := coordmetadata.New(t.Context(), &coordoption.Options{
		Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: coordoption.FileMetadata{
					Dir: dir,
				},
			},
		},
	})
	require.NoError(t, err)

	metadata, err := metadataFactory.CreateMetadata(t.Context())
	require.NoError(t, err)
	return metadataFactory, metadata
}

func mirrorProviderToFile[T interface {
	gproto.Message
	*proto.ClusterStatus | *proto.ClusterConfiguration
}](
	t *testing.T,
	path string,
	codec metadatacodec.Codec[T],
	source provider.Provider[T],
) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	receiver := source.Watch().Subscribe()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-receiver.Changed():
				if err := writeSnapshotFile(path, codec, receiver.Load().Value); err != nil {
					panic(err)
				}
			}
		}
	}()
}

func writeSnapshot[T interface {
	gproto.Message
	*proto.ClusterStatus | *proto.ClusterConfiguration
}](
	t *testing.T,
	path string,
	codec metadatacodec.Codec[T],
	value T,
) {
	t.Helper()

	require.NoError(t, writeSnapshotFile(path, codec, value))
}

func writeSnapshotFile[T interface {
	gproto.Message
	*proto.ClusterStatus | *proto.ClusterConfiguration
}](
	path string,
	codec metadatacodec.Codec[T],
	value T,
) error {
	data, err := codec.MarshalYAML(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, metadataFilePerm)
}
