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

package scheduler

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
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

func TestSchedulerDrainsARemovedDataServer(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	s := start(t, metadata)

	change := (<-s.Actions()).(*action.ChangeEnsembleAction)
	assert.Equal(t, "r", change.From.GetNameOrDefault())

	// The outcome of the op starts the next round, which finds nothing to do
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities(change.To.GetInternal(), "n0")}})
	change.Done(nil)
	assert.Never(t, func() bool { return len(s.Actions()) > 0 }, time.Second, 10*time.Millisecond)
}

func TestSchedulerRunsARoundOnAChangeOfTheConfiguration(t *testing.T) {
	metadata := newFakeMetadata(t)
	s := start(t, metadata)
	require.Eventually(t, metadata.subscribed, 5*time.Second, time.Millisecond)

	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	metadata.changeConfig(t)

	<-s.Actions()
}

func TestSchedulerRunsARoundEveryInterval(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.SchedulerPolicy = &proto.SchedulerPolicy{Interval: "10ms"}
	s := start(t, metadata)

	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})

	<-s.Actions()
}

func TestSchedulerKeepsRunningWhenTheMetadataCloses(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.SchedulerPolicy = &proto.SchedulerPolicy{Interval: "10ms"}
	s := start(t, metadata)
	require.Eventually(t, metadata.subscribed, 5*time.Second, time.Millisecond)

	require.NoError(t, metadata.configCache.Close())
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})

	<-s.Actions()
}

func TestSchedulerRoundSkipsAFailedObservation(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.listDataServerErr = errors.New("unavailable")
	s := New(Options{Context: t.Context(), Metadata: metadata, Health: healthy}).(*scheduler)

	assert.Equal(t, policy.DefaultInterval, s.round(nil))
	assert.Empty(t, s.Actions())
}

func TestSchedulerRoundFinishesAFlightWithoutThePolicy(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	s := New(Options{Context: t.Context(), Metadata: metadata, Health: healthy}).(*scheduler)
	s.round(nil)
	change := <-s.Actions()

	metadata.getConfigErr = errors.New("unavailable")
	go change.Error(errors.New("not ready"))
	assert.Equal(t, policy.DefaultInterval, s.round(<-s.dispatcher.Results()))

	assert.True(t, s.dispatcher.Busy(1))
}

func TestSchedulerRoundLogsStuckReplicas(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r")}})
	s := New(Options{Context: t.Context(), Metadata: metadata, Health: healthy}).(*scheduler)

	s.round(nil)

	assert.Empty(t, s.Actions())
}

func TestSchedulerObserveBuildsTheSnapshot(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.dataServers["n0"].Metadata = &proto.DataServerMetadata{Labels: map[string]string{"zone": "z0"}}
	zones := []*proto.AntiAffinity{{Labels: []string{"zone"}}}
	metadata.namespaces["a"] = &proto.Namespace{Name: "a", AntiAffinities: zones}
	metadata.shards["a"] = map[int64]*proto.ShardMetadata{
		1: {Ensemble: identities("r", "n0"), Leader: identities("n0")[0]},
		2: {Ensemble: identities("n0", "n1"), Split: &proto.SplitMetadata{}},
		3: {Ensemble: identities("n0", "n1"), Status: proto.ShardStatusDeleting},
		4: {Ensemble: identities("n1", "n2")},
		6: {Ensemble: identities("n0", "n2"), EnsembleChange: &proto.EnsembleChangeMetadata{}},
	}
	metadata.shards["orphan"] = map[int64]*proto.ShardMetadata{5: {Ensemble: identities("n1", "n2")}}

	s := New(Options{Context: t.Context(), Metadata: metadata,
		Health: func(name string) (bool, time.Time) {
			// n0 has run for long, n1 just started, n2 is down
			if name == "n0" {
				return true, time.Time{}
			}
			return name != "n2", time.Now()
		}}).(*scheduler)

	snapshot, err := s.observe(policy.New(nil), func(shard int64) bool { return shard == 4 })

	require.NoError(t, err)
	assert.Equal(t, &model.DataServer{Identity: identities("n0")[0], Labels: map[string]string{"zone": "z0"},
		Running: true, Stable: true}, snapshot.DataServers["n0"])
	assert.Equal(t, &model.DataServer{Identity: identities("n2")[0]}, snapshot.DataServers["n2"])
	assert.Equal(t, &model.DataServer{Identity: identities("r")[0], Removed: true, Running: true},
		snapshot.DataServers["r"])
	assert.Equal(t, &model.Namespace{AntiAffinities: zones, Shards: map[int64]*model.Shard{
		1: {Ensemble: []string{"r", "n0"}, Leader: "n0"},
		2: {Ensemble: []string{"n0", "n1"}, Frozen: true},
		4: {Ensemble: []string{"n1", "n2"}, Frozen: true},
		6: {Ensemble: []string{"n0", "n2"}, Frozen: true, Changing: true},
	}}, snapshot.Namespaces["a"])
	assert.Nil(t, snapshot.Namespaces["orphan"].AntiAffinities)
}

func TestSchedulerObserveReturnsTheMetadataErrors(t *testing.T) {
	for _, set := range []func(*fakeMetadata, error){
		func(m *fakeMetadata, err error) { m.listDataServerErr = err },
		func(m *fakeMetadata, err error) { m.listNamespaceErr = err },
		func(m *fakeMetadata, err error) { m.listNamespaceStatusErr = err },
	} {
		metadata := newFakeMetadata(t)
		want := errors.New("unavailable")
		set(metadata, want)

		s := New(Options{Context: t.Context(), Metadata: metadata, Health: healthy}).(*scheduler)

		_, err := s.observe(policy.New(nil), func(int64) bool { return false })

		assert.ErrorIs(t, err, want)
	}
}

func start(t *testing.T, metadata *fakeMetadata) Scheduler {
	t.Helper()
	s := New(Options{Context: t.Context(), Metadata: metadata, Health: healthy})
	s.Start()
	t.Cleanup(func() { assert.NoError(t, s.Close()) })
	return s
}

// healthy reports every data server running for long.
func healthy(string) (running bool, since time.Time) {
	return true, time.Time{}
}

func identities(names ...string) []*proto.DataServerIdentity {
	ids := make([]*proto.DataServerIdentity, 0, len(names))
	for _, name := range names {
		ids = append(ids, &proto.DataServerIdentity{Internal: name})
	}
	return ids
}

// fakeMetadata serves the reads of the scheduler, and notifies the changes of
// the configuration; the other methods of the interface panic.
type fakeMetadata struct {
	coordmetadata.Metadata

	mu            sync.Mutex
	config        *proto.ClusterConfiguration
	dataServers   map[string]*proto.DataServer
	namespaces    map[string]*proto.Namespace
	shards        map[string]map[int64]*proto.ShardMetadata
	subscriptions int
	configCache   *cache.Cache[provider.Versioned[*proto.ClusterConfiguration]]

	getConfigErr           error
	listDataServerErr      error
	listNamespaceErr       error
	listNamespaceStatusErr error
}

// newFakeMetadata has the data servers n0 to n2 in its configuration.
func newFakeMetadata(t *testing.T) *fakeMetadata {
	t.Helper()
	m := &fakeMetadata{config: &proto.ClusterConfiguration{}, dataServers: make(map[string]*proto.DataServer),
		namespaces: make(map[string]*proto.Namespace), shards: make(map[string]map[int64]*proto.ShardMetadata)}
	for _, id := range identities("n0", "n1", "n2") {
		m.dataServers[id.GetInternal()] = &proto.DataServer{Identity: id}
	}
	m.configCache = cache.New(t.Context(),
		func(context.Context) (*provider.Versioned[*proto.ClusterConfiguration], error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			return &provider.Versioned[*proto.ClusterConfiguration]{Value: m.config}, nil
		}, nil)
	t.Cleanup(func() { _ = m.configCache.Close() })
	return m
}

func (m *fakeMetadata) setShards(namespace string, shards map[int64]*proto.ShardMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[namespace] = shards
}

func (m *fakeMetadata) changeConfig(t *testing.T) {
	t.Helper()
	require.NoError(t, m.configCache.Reload())
}

func (m *fakeMetadata) subscribed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subscriptions > 0
}

func (m *fakeMetadata) SubscribeConfig() *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]] {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscriptions++
	return m.configCache.Subscribe()
}

func (m *fakeMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return commonobject.Borrow(m.config), m.getConfigErr
}

func (m *fakeMetadata) ListDataServer() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.DataServer], len(m.dataServers))
	for name, dataServer := range m.dataServers {
		result[name] = commonobject.Borrow(dataServer)
	}
	return result, m.listDataServerErr
}

func (m *fakeMetadata) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.Namespace], len(m.namespaces))
	for name, namespace := range m.namespaces {
		result[name] = commonobject.Borrow(namespace)
	}
	return result, m.listNamespaceErr
}

func (m *fakeMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(m.shards))
	for name, shards := range m.shards {
		result[name] = commonobject.Borrow(&proto.NamespaceStatus{Shards: shards})
	}
	return result, m.listNamespaceStatusErr
}
