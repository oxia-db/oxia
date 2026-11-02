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

package collector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

const mb = 1024 * 1024

func TestCollectorCollectReturnsTheSnapshot(t *testing.T) {
	n0, n1 := &proto.DataServerIdentity{Internal: "n0"}, &proto.DataServerIdentity{Internal: "n1"}
	metadata := &fakeMetadata{
		dataServers: map[string]commonobject.Borrowed[*proto.DataServer]{
			"n0": commonobject.Borrow(&proto.DataServer{Identity: n0}),
			"n1": commonobject.Borrow(&proto.DataServer{Identity: n1}),
		},
		statuses: map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
			"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
				1: {Ensemble: []*proto.DataServerIdentity{n0}, Leader: n0},
			}}),
		},
	}
	// n1 doesn't answer
	running := func(name string) bool { return name == "n0" }

	s, err := New(t.Context(), metadata, running, nil).Collect()

	require.NoError(t, err)
	assert.Equal(t, &stats.ShardStats{Ensemble: []string{"n0"}, Leader: "n0"}, s.GetShard(1))
	assert.False(t, s.DataServers["n0"].Down)
	assert.True(t, s.DataServers["n1"].Down)
}

func TestCollectorCollectTakesTheLabelsOfARemovedDataServerFromItsStatus(t *testing.T) {
	n0, removed := &proto.DataServerIdentity{Internal: "n0"}, &proto.DataServerIdentity{Internal: "removed"}
	labels := &proto.DataServerMetadata{Labels: map[string]string{"zone": "z1"}}
	metadata := &fakeMetadata{
		dataServers: map[string]commonobject.Borrowed[*proto.DataServer]{
			"n0": commonobject.Borrow(&proto.DataServer{Identity: n0}),
		},
		dataServerStatuses: map[string]commonobject.Borrowed[*proto.DataServerStatus]{
			"removed": commonobject.Borrow(&proto.DataServerStatus{
				State: proto.DataServerState_DATA_SERVER_STATE_DRAINING, Metadata: labels}),
		},
		statuses: map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
			"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
				1: {Ensemble: []*proto.DataServerIdentity{n0, removed}, Leader: n0},
			}}),
		},
	}

	s, err := New(t.Context(), metadata, func(string) bool { return true }, nil).Collect()

	require.NoError(t, err)
	assert.True(t, s.DataServers["removed"].Removed)
	assert.Equal(t, removed, s.DataServers["removed"].GetIdentity())
	assert.Equal(t, labels, s.DataServers["removed"].GetMetadata())
}

func TestCollectorCollectReturnsAFailedRead(t *testing.T) {
	for name, metadata := range map[string]*fakeMetadata{
		"data servers":         {listDataServerErr: errors.New("unavailable")},
		"data server statuses": {listDataServerStatusErr: errors.New("unavailable")},
		"namespaces":           {listNamespaceErr: errors.New("unavailable")},
		"statuses":             {listNamespaceStatusErr: errors.New("unavailable")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(t.Context(), metadata, nil, nil).Collect()
			assert.Error(t, err)
		})
	}
}

func TestCollectorSampleSetsTheOvershoot(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {DbSizeBytes: 3 * mb}}}
	c := New(t.Context(), nil, nil, provider)
	s := newStats()

	c.Sample(s, split(), time.Now())

	assert.InDelta(t, 3, s.GetShard(1).Overshoot, 0.001)
	assert.True(t, s.GetShard(1).Splittable)
}

func TestCollectorSampleWaitsTheStabilizationPeriod(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {DbSizeBytes: 3 * mb}}}
	c := New(t.Context(), nil, nil, provider)
	p := split()
	p.Stabilization = time.Minute
	now := time.Now()

	for _, after := range []time.Duration{0, 30 * time.Second} {
		s := newStats()
		c.Sample(s, p, now.Add(after))
		assert.Zero(t, s.GetShard(1).Overshoot, after)
	}
	s := newStats()
	c.Sample(s, p, now.Add(time.Minute))
	assert.InDelta(t, 3, s.GetShard(1).Overshoot, 0.001)

	// A gap of more than two collection intervals starts the period over
	s = newStats()
	c.Sample(s, p, now.Add(3*time.Minute))
	assert.Zero(t, s.GetShard(1).Overshoot)
}

func TestCollectorSampleMeasuresTheThroughputOfALeader(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {ReadOpsTotal: 100}}}
	c := New(t.Context(), nil, nil, provider)
	p := split()
	now := time.Now()
	sample := func(leader string, after time.Duration, ops uint64) float64 {
		provider.set(1, &proto.ShardStats{ReadOpsTotal: ops / 2, WriteOpsTotal: ops - ops/2})
		s := newStats()
		s.GetShard(1).Leader = leader
		c.Sample(s, p, now.Add(after))
		return s.GetShard(1).Overshoot
	}

	assert.Zero(t, sample("n0", 0, 100), "a first sample has no rate")
	assert.InDelta(t, 2, sample("n0", 30*time.Second, 100+30*20), 0.001)
	assert.Zero(t, sample("n1", 60*time.Second, 2000), "a new leader has other counters")
	assert.InDelta(t, 2, sample("n1", 90*time.Second, 2000+30*20), 0.001)
	assert.Zero(t, sample("n1", 120*time.Second, 10), "the counters went back, after a restart")
}

func TestCollectorSampleSamplesOncePerCollectionInterval(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {}}}
	c := New(t.Context(), nil, nil, provider)
	now := time.Now()

	c.Sample(newStats(), split(), now)
	c.Sample(newStats(), split(), now.Add(time.Second))
	assert.Equal(t, 1, provider.calls())

	c.Sample(newStats(), split(), now.Add(30*time.Second))
	assert.Equal(t, 2, provider.calls())
}

func TestCollectorSampleForgetsTheSamplesWhenDisabled(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {DbSizeBytes: 3 * mb}}}
	c := New(t.Context(), nil, nil, provider)
	c.Sample(newStats(), split(), time.Now())
	require.Len(t, c.trackers, 1)

	p := split()
	p.Enabled = false
	s := newStats()
	c.Sample(s, p, time.Now())

	assert.Empty(t, c.trackers)
	assert.Zero(t, s.GetShard(1).Overshoot)
	assert.Equal(t, 1, provider.calls())
}

func TestCollectorSampleForgetsTheShardsThatAreGone(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {}}}
	c := New(t.Context(), nil, nil, provider)
	now := time.Now()
	c.Sample(newStats(), split(), now)
	require.Len(t, c.trackers, 1)

	s := newStats()
	delete(s.Namespaces["a"].Shards, 1)
	c.Sample(s, split(), now.Add(30*time.Second))

	assert.Empty(t, c.trackers)
}

func TestCollectorSampleSkipsTheShardsWithoutStats(t *testing.T) {
	provider := &fakeProvider{err: errors.New("unavailable")}
	c := New(t.Context(), nil, nil, provider)

	s := newStats()
	c.Sample(s, split(), time.Now())
	assert.Zero(t, s.GetShard(1).Overshoot)
	assert.Equal(t, 1, provider.calls())

	// A leader that isn't a known data server isn't asked
	s = newStats()
	s.GetShard(1).Leader = "x"
	c.Sample(s, split(), time.Now().Add(time.Minute))
	assert.Equal(t, 1, provider.calls())
}

func TestCollectorSampleHoldsTheSplitsDuringTheCooldown(t *testing.T) {
	provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {DbSizeBytes: 3 * mb}}}
	c := New(t.Context(), nil, nil, provider)
	p := split()
	p.Cooldown = time.Minute
	now := time.Now()
	c.SetLastSplit(now)

	s := newStats()
	c.Sample(s, p, now.Add(30*time.Second))
	assert.Zero(t, s.GetShard(1).Overshoot)

	s = newStats()
	c.Sample(s, p, now.Add(time.Minute))
	assert.InDelta(t, 3, s.GetShard(1).Overshoot, 0.001)
}

func TestCollectorSampleKeepsTheShardsFromSplitting(t *testing.T) {
	for name, tc := range map[string]struct {
		maxShards int
		deleting  int
		filtering bool
	}{
		"namespace full":             {maxShards: 1},
		"shard being deleted counts": {maxShards: 2, deleting: 1},
		"filtering its parent":       {maxShards: 10, filtering: true},
	} {
		t.Run(name, func(t *testing.T) {
			provider := &fakeProvider{stats: map[int64]*proto.ShardStats{1: {SplitFilterPending: tc.filtering}}}
			p := split()
			p.MaxShardsPerNamespace = tc.maxShards
			s := newStats()
			s.Namespaces["a"].DeletingShards = tc.deleting

			New(t.Context(), nil, nil, provider).Sample(s, p, time.Now())

			assert.False(t, s.GetShard(1).Splittable)
		})
	}
}

// newStats has the data servers n0 and n1, and the splittable shard 1 of
// namespace a, led by n0.
func newStats() *stats.ClusterStats {
	dataServer := func(name string) *stats.DataServerStats {
		return &stats.DataServerStats{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Internal: name}}}
	}
	return &stats.ClusterStats{
		DataServers: map[string]*stats.DataServerStats{"n0": dataServer("n0"), "n1": dataServer("n1")},
		Namespaces: map[string]*stats.NamespaceStats{"a": {Shards: map[int64]*stats.ShardStats{
			1: {Ensemble: []string{"n0", "n1"}, Leader: "n0", Splittable: true},
		}}},
	}
}

// split splits a shard over 1 MB or 10 ops/s right away, sampled every 30s.
func split() policy.Split {
	return policy.Split{Enabled: true, MaxShardSizeMB: 1, MaxThroughputOps: 10, MaxShardsPerNamespace: 10,
		CollectionInterval: 30 * time.Second}
}

// fakeMetadata serves the reads of Collect; the other methods of the
// interface panic.
type fakeMetadata struct {
	coordmetadata.Metadata

	dataServers        map[string]commonobject.Borrowed[*proto.DataServer]
	dataServerStatuses map[string]commonobject.Borrowed[*proto.DataServerStatus]
	statuses           map[string]commonobject.Borrowed[*proto.NamespaceStatus]

	listDataServerErr       error
	listDataServerStatusErr error
	listNamespaceErr        error
	listNamespaceStatusErr  error
}

func (m *fakeMetadata) ListDataServer() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
	return m.dataServers, m.listDataServerErr
}

func (m *fakeMetadata) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	return nil, m.listNamespaceErr
}

func (m *fakeMetadata) ListDataServerStatus() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error) {
	return m.dataServerStatuses, m.listDataServerStatusErr
}

func (m *fakeMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	return m.statuses, m.listNamespaceStatusErr
}

// fakeProvider answers GetStatus with the stats of each shard, or err; the
// other methods of the interface panic.
type fakeProvider struct {
	rpc.ControllerRPCProvider

	mu      sync.Mutex
	stats   map[int64]*proto.ShardStats
	err     error
	counter int
}

func (p *fakeProvider) set(shard int64, shardStats *proto.ShardStats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats[shard] = shardStats
}

func (p *fakeProvider) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counter
}

func (p *fakeProvider) GetStatus(_ context.Context, _ *proto.DataServerIdentity,
	req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter++
	if p.err != nil {
		return nil, p.err
	}
	return &proto.GetStatusResponse{ShardStats: p.stats[req.GetShard()]}, nil
}
