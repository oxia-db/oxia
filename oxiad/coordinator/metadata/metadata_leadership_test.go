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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadataconstant "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

// supersededStatusProvider grants the leadership, and once superseded refuses
// every status write on the version, as when another coordinator took over
// and wrote the status.
type supersededStatusProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
	lost       chan struct{}
	superseded *atomic.Bool
}

func (p supersededStatusProvider) WaitToBecomeLeader() (<-chan struct{}, error) {
	return p.lost, nil
}

func (p supersededStatusProvider) Store(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
	if p.superseded.Load() {
		return metadataconstant.NotExists, metadataconstant.ErrBadVersion
	}
	return p.Provider.Store(snapshot)
}

func newLeaderMetadata(t *testing.T) (Metadata, supersededStatusProvider) {
	t.Helper()

	statusProvider := supersededStatusProvider{
		Provider:   memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
		lost:       make(chan struct{}),
		superseded: &atomic.Bool{},
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	return metadata, statusProvider
}

// A coordinator that lost the leadership can still have a status write in
// flight. The new leader's writes refuse it on the version: the write fails
// instead of taking the whole process down.
func TestMetadataStatusWriteAfterLeadershipLossFails(t *testing.T) {
	metadata, statusProvider := newLeaderMetadata(t)

	close(statusProvider.lost)
	statusProvider.superseded.Store(true)

	_, err := metadata.AllocateShardIDs(1)
	require.ErrorIs(t, err, metadataconstant.ErrBadVersion)
}

// A new leader can write the status before this coordinator learns that it
// lost the leadership. The version check already refused the stale write: it
// fails, and the process keeps running.
func TestMetadataStatusVersionConflictWhileLeadingFails(t *testing.T) {
	metadata, statusProvider := newLeaderMetadata(t)

	statusProvider.superseded.Store(true)

	require.NotPanics(t, func() {
		_, err := metadata.AllocateShardIDs(1)
		require.ErrorIs(t, err, metadataconstant.ErrBadVersion)
	})
}
