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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

type interceptingStatusProvider struct {
	provider.Provider[*proto.ClusterStatus]
	store func(provider.Versioned[*proto.ClusterStatus]) (metadatacommon.Version, error)
}

func (p interceptingStatusProvider) Store(snapshot provider.Versioned[*proto.ClusterStatus]) (metadatacommon.Version, error) {
	return p.store(snapshot)
}

func TestCreateNamespaceStatusAlreadyExists(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	require.NoError(t, metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{ReplicationFactor: 3}))
	version := statusProvider.Watch().Load().Version
	err := metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{ReplicationFactor: 1})
	require.ErrorIs(t, err, metadatacommon.ErrAlreadyExists)
	require.Contains(t, err.Error(), "default")
	require.Equal(t, version, statusProvider.Watch().Load().Version)
	status, exists := metadata.GetNamespaceStatus("default")
	require.True(t, exists)
	require.EqualValues(t, 3, status.UnsafeBorrow().GetReplicationFactor())
}

func TestCreateNamespaceStatusReturnsWriteError(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	storeErr := errors.New("status store unavailable")
	writes := 0
	failing := interceptingStatusProvider{
		Provider: statusProvider,
		store: func(snapshot provider.Versioned[*proto.ClusterStatus]) (metadatacommon.Version, error) {
			writes++
			if writes > 1 {
				return statusProvider.Store(snapshot)
			}
			return metadatacommon.NotExists, storeErr
		},
	}
	metadata := newMetadata(t.Context(), failing, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	err := metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{ReplicationFactor: 3})
	require.ErrorIs(t, err, storeErr)
	require.NotErrorIs(t, err, metadatacommon.ErrAlreadyExists)
	require.Equal(t, 1, writes)
	_, exists := metadata.GetNamespaceStatus("default")
	require.False(t, exists)
	require.Equal(t, metadatacommon.NotExists, statusProvider.Watch().Load().Version)
}

func TestCreateNamespaceStatusCanBeRetriedByCaller(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	writes := 0
	storeErr := errors.New("temporary status store failure")
	failingOnce := interceptingStatusProvider{
		Provider: statusProvider,
		store: func(snapshot provider.Versioned[*proto.ClusterStatus]) (metadatacommon.Version, error) {
			writes++
			if writes == 1 {
				return metadatacommon.NotExists, storeErr
			}
			return statusProvider.Store(snapshot)
		},
	}
	metadata := newMetadata(t.Context(), failingOnce, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	namespaceStatus := &proto.NamespaceStatus{ReplicationFactor: 3}
	err := metadata.CreateNamespaceStatus("default", namespaceStatus)
	require.ErrorIs(t, err, storeErr)
	require.Equal(t, 1, writes)
	_, exists := metadata.GetNamespaceStatus("default")
	require.False(t, exists)

	require.NoError(t, metadata.CreateNamespaceStatus("default", namespaceStatus))
	require.Equal(t, 2, writes)
	status, exists := metadata.GetNamespaceStatus("default")
	require.True(t, exists)
	require.EqualValues(t, 3, status.UnsafeBorrow().GetReplicationFactor())
}

func TestCreateNamespaceStatusReturnsErrorAfterCommit(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	storeErr := errors.New("metadata write timed out after committing")
	writes := 0
	committing := interceptingStatusProvider{
		Provider: statusProvider,
		store: func(snapshot provider.Versioned[*proto.ClusterStatus]) (metadatacommon.Version, error) {
			writes++
			version, err := statusProvider.Store(snapshot)
			if err != nil {
				return version, err
			}
			return version, storeErr
		},
	}
	metadata := newMetadata(t.Context(), committing, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	err := metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{ReplicationFactor: 3})
	require.ErrorIs(t, err, storeErr)
	saved, exists := metadata.GetNamespaceStatus("default")
	require.True(t, exists)
	require.EqualValues(t, 3, saved.UnsafeBorrow().ReplicationFactor)
	require.NotEqual(t, metadatacommon.NotExists, statusProvider.Watch().Load().Version)

	require.ErrorIs(t, metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{}), metadatacommon.ErrAlreadyExists)
	require.Equal(t, 1, writes)
}
