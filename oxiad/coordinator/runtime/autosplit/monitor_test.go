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

	"github.com/pkg/errors"
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
	mu      sync.Mutex
	splits  []splitCall
	failFor map[int64]bool // shard IDs for which InitiateSplit returns an error
}

type splitCall struct {
	namespace string
	shardId   int64
}

func (s *mockSplitter) InitiateSplit(namespace string, parentShardId int64, _ *uint32) (leftChild, rightChild int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failFor[parentShardId] {
		return 0, 0, errors.Errorf("cannot split shard %d", parentShardId)
	}
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

// steadyShard is a helper for building a steady-state shard with a leader and a
// full hash range.
func steadyShard(leader *proto.DataServerIdentity, minHash, maxHash uint32) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         proto.ShardStatusSteadyState,
		Term:           1,
		Leader:         leader,
		Ensemble:       []*proto.DataServerIdentity{leader},
		Int32HashRange: &proto.HashRange{Min: minHash, Max: maxHash},
	}
}

// A malformed cooldown_period must fall back to the default, NOT silently
// disable the cooldown (which a discarded parse error -> 0 would do).
func TestMonitor_MalformedCooldownFallsBackToDefault(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxShardSizeMb:      100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "5min", // invalid Go duration; correct is "5m"
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: steadyShard(leader, 0, 2147483647),
			1: steadyShard(leader, 2147483648, 4294967295),
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})
	rpcMock.setStats(1, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()
	require.Len(t, splitter.getSplits(), 1)

	// With the parse error correctly defaulting to 5m (not 0), the cooldown
	// blocks the second split. A regression (cooldown=0) would split shard 1.
	m.evaluate()
	assert.Len(t, splitter.getSplits(), 1)
}

// If the worst-overshoot shard cannot be split, the monitor must fall through
// to the next candidate rather than getting wedged on it forever.
func TestMonitor_StarvationFallThrough(t *testing.T) {
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
			0: steadyShard(leader, 0, 2147483647), // worst offender, but unsplittable
			1: steadyShard(leader, 2147483648, 4294967295),
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 500 * 1024 * 1024}) // higher overshoot
	rpcMock.setStats(1, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{failFor: map[int64]bool{0: true}}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()

	splits := splitter.getSplits()
	require.Len(t, splits, 1)
	assert.Equal(t, int64(1), splits[0].shardId) // fell through to shard 1
}

// A shard whose hash range is too small to split must be pre-filtered so it is
// never even attempted (and cannot block other candidates).
func TestMonitor_UnsplittableShardSkipped(t *testing.T) {
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
			0: steadyShard(leader, 100, 100), // width 0 -> unsplittable
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 500 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	m.evaluate()
	assert.Empty(t, splitter.getSplits())
}

// A leader change resets the throughput baseline so a cross-leader counter
// delta cannot be mistaken for a genuine spike and trigger a wrong split.
func TestMonitor_LeaderChangeResetsThroughput(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxThroughputOps:    100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leaderA := &proto.DataServerIdentity{Name: strPtr("nodeA"), Public: "a:9091", Internal: "a:8191"}
	leaderB := &proto.DataServerIdentity{Name: strPtr("nodeB"), Public: "b:9091", Internal: "b:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: steadyShard(leaderA, 0, 4294967295),
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 1000, WriteOpsTotal: 1000})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	// Establish a baseline under leader A.
	m.evaluate()
	require.Empty(t, splitter.getSplits())

	// Leader moves to B, whose cumulative counters are much higher. Age the
	// sample so elapsed > 0.
	metadata.UpdateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: steadyShard(leaderB, 0, 4294967295),
		},
	})
	m.mu.Lock()
	m.trackers[shardKey{namespace: "default", shardId: 0}].lastSampleTime = time.Now().Add(-time.Second)
	m.mu.Unlock()
	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 5000000, WriteOpsTotal: 5000000})

	// Because the leader changed, the huge cross-leader delta must be ignored.
	m.evaluate()
	assert.Empty(t, splitter.getSplits())
}

// A same-identity leader restart resets the cumulative counters; the derived
// throughput must be dropped rather than carried forward as a stale spike.
func TestMonitor_CounterResetDropsThroughput(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:             true,
			MaxThroughputOps:    100,
			StabilizationPeriod: "0s",
			CooldownPeriod:      "0s",
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Name: strPtr("nodeA"), Public: "a:9091", Internal: "a:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: steadyShard(leader, 0, 4294967295),
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	// Two same-leader samples establish a high throughput rate.
	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 0, WriteOpsTotal: 0})
	m.evaluate()
	key := shardKey{namespace: "default", shardId: 0}
	m.mu.Lock()
	m.trackers[key].lastSampleTime = time.Now().Add(-time.Second)
	m.mu.Unlock()
	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 500, WriteOpsTotal: 500})
	m.evaluate()
	require.Len(t, splitter.getSplits(), 1) // 1000 ops/s > 100 -> split

	// Same leader, counters drop to near-zero (process restart). The stale high
	// rate must be dropped by the code, not carried forward.
	rpcMock.setStats(0, &proto.ShardStats{ReadOpsTotal: 1, WriteOpsTotal: 1})
	m.mu.Lock()
	m.trackers[key].lastSampleTime = time.Now().Add(-time.Second)
	m.mu.Unlock()

	m.evaluate()

	m.mu.Lock()
	got := m.trackers[key].throughputOps
	m.mu.Unlock()
	assert.Equal(t, float64(0), got, "throughput must reset when counters go backwards")
}

// Trackers for a namespace that is temporarily skipped at the shard-count cap
// must be retained (their stabilization history not pruned).
func TestMonitor_MaxShardCountRetainsTrackers(t *testing.T) {
	config := &proto.ClusterConfiguration{
		AutoSplit: &proto.AutoSplitConfig{
			Enabled:               true,
			MaxShardSizeMb:        100,
			StabilizationPeriod:   "0s",
			CooldownPeriod:        "0s",
			MaxShardsPerNamespace: 2,
		},
	}
	metadata := newTestMetadata(t, config)
	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			0: steadyShard(leader, 0, 2147483647),
			1: steadyShard(leader, 2147483648, 4294967295),
		},
	})

	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	rpcMock.setStats(0, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})
	rpcMock.setStats(1, &proto.ShardStats{DbSizeBytes: 200 * 1024 * 1024})

	splitter := &mockSplitter{}
	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)

	// Seed a tracker for shard 0 directly, then evaluate: the namespace is at
	// the cap (2 >= 2) so it is skipped, but the tracker must survive.
	m.mu.Lock()
	m.trackers[shardKey{namespace: "default", shardId: 0}] = &shardStatsTracker{exceedingSince: time.Now()}
	m.mu.Unlock()

	m.evaluate()

	m.mu.Lock()
	_, retained := m.trackers[shardKey{namespace: "default", shardId: 0}]
	m.mu.Unlock()
	assert.True(t, retained, "tracker for a shard in a capped namespace must not be pruned")
	assert.Empty(t, splitter.getSplits())
}

// Start()/Close() must run and shut down the background loop cleanly.
func TestMonitor_StartCloseLifecycle(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	rpcMock := &mockRpcProvider{statuses: make(map[int64]*proto.GetStatusResponse)}
	splitter := &mockSplitter{}

	m := NewMonitor(metadata, rpcMock, splitter, time.Millisecond)
	m.Start()
	time.Sleep(10 * time.Millisecond) // let a few ticks run
	m.Close()                         // must return without hanging or panicking
}

func strPtr(s string) *string { return &s }
