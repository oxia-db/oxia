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

package autosplit

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

type mockRpcProvider struct {
	mu       sync.Mutex
	statuses map[int64]*proto.GetStatusResponse
}

func (m *mockRpcProvider) GetStatus(_ context.Context, _ *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if resp, ok := m.statuses[req.Shard]; ok {
		return resp, nil
	}
	return &proto.GetStatusResponse{}, nil
}

func (m *mockRpcProvider) setStats(shard int64, stats *proto.ShardStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses[shard] = &proto.GetStatusResponse{
		Term:       1,
		Status:     proto.ServingStatus_LEADER,
		ShardStats: stats,
	}
}

// mockRpcProvider implements autosplit.StatusGetter.

type mockSplitter struct {
	mu     sync.Mutex
	splits []splitCall
}

type splitCall struct {
	namespace string
	shardId   int64
}

func (s *mockSplitter) InitiateSplit(namespace string, parentShardId int64, _ *uint32) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.splits = append(s.splits, splitCall{namespace, parentShardId})
	return parentShardId + 100, parentShardId + 101, nil
}

func (s *mockSplitter) getSplits() []splitCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]splitCall, len(s.splits))
	copy(result, s.splits)
	return result
}

func newTestMetadata(t *testing.T, config *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	dir := t.TempDir()
	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileConfigName), data, 0o600))

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
	_, err = metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, metadataFactory.Close())
	})
	return metadata
}

func TestMonitor_DisabledByDefault(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	splitter := &mockSplitter{}

	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)
	m.evaluate()

	assert.Empty(t, splitter.getSplits())
}

func TestMonitor_SplitOnSizeThreshold(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxShardSizeMb:      100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 4294967295},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()

	splits := splitter.getSplits()
	require.Len(t, splits, 1)
	assert.Equal(t, "default", splits[0].namespace)
	assert.Equal(t, int64(0), splits[0].shardId)
}

func TestMonitor_StabilizationPeriod(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxShardSizeMb:      100,
			StabilizationPeriod: "1h",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 4294967295},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()
	assert.Empty(t, splitter.getSplits())
}

func TestMonitor_CooldownPreventsRapidSplits(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxShardSizeMb:      100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "1h",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 2147483647},
			},
			1: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 2147483648, Max: 4294967295},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})
	rpcMock.setStats(1, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()
	require.Len(t, splitter.getSplits(), 1)

	// Second evaluation blocked by cooldown
	m.evaluate()
	assert.Len(t, splitter.getSplits(), 1)
}

func TestMonitor_SkipsWhenSplitInProgress(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxShardSizeMb:      100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 4294967295},
				Split: &proto.SplitMetadata{
					Phase:         proto.SplitPhaseBootstrap,
					ChildShardIds: []int64{10, 11},
				},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()
	assert.Empty(t, splitter.getSplits())
}

func TestMonitor_MaxShardsGuardRail(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:               true,
			MaxShardSizeMb:        100,
			StabilizationPeriod:   "0s",
			CooldownPeriod:        "0s",
			MaxShardsPerNamespace: 1,
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 4294967295},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()
	assert.Empty(t, splitter.getSplits())
}

func TestMonitor_ThroughputThreshold(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxThroughputOps:    100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 4294967295},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	// First sample — no rate yet
	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 0, WriteOpsTotal: 0})
	m.evaluate()
	assert.Empty(t, splitter.getSplits())

	// Simulate time passing with high ops delta
	m.mu.Lock()
	tracker := m.trackers[shardKey{namespace: "default", shardId: 0}]
	tracker.lastSampleTime = time.Now().Add(-1 * time.Second)
	m.mu.Unlock()

	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 100, WriteOpsTotal: 100})
	m.evaluate()

	require.Len(t, splitter.getSplits(), 1)
}

func TestMonitor_PicksWorstOffender(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxShardSizeMb:      100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 2147483647},
			},
			1: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         leader,
				Ensemble:       []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{Min: 2147483648, Max: 4294967295},
			},
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})
	rpcMock.setStats(1, &proto.ShardStats{DbSizeBytes: 500 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()

	splits := splitter.getSplits()
	require.Len(t, splits, 1)
	assert.Equal(t, int64(1), splits[0].shardId)
}
