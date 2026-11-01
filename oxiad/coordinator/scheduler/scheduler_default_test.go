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
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/metric"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/collector"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestSchedulerDrainsARemovedDataServer(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "10ms"}
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	s := start(t, metadata)

	change := (<-s.GetActions()).(*action.ChangeEnsembleAction)
	assert.Equal(t, "r", change.From.GetNameOrDefault())

	// The metadata shows the op once it is done: the next round finds nothing
	// to do
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities(change.To.GetInternal(), "n0")}})
	change.Done(nil)
	assert.Never(t, func() bool { return len(s.GetActions()) > 0 }, time.Second, 10*time.Millisecond)
}

func TestSchedulerRunsARoundOnAChangeOfTheConfiguration(t *testing.T) {
	metadata := newFakeMetadata(t)
	s := start(t, metadata)
	require.Eventually(t, metadata.subscribed, 5*time.Second, time.Millisecond)

	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	metadata.changeConfig(t)

	<-s.GetActions()
}

func TestSchedulerRunsARoundEveryInterval(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "10ms"}
	s := start(t, metadata)

	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})

	<-s.GetActions()
}

func TestSchedulerKeepsRunningWhenTheMetadataCloses(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "10ms"}
	s := start(t, metadata)
	require.Eventually(t, metadata.subscribed, 5*time.Second, time.Millisecond)

	require.NoError(t, metadata.configCache.Close())
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})

	<-s.GetActions()
}

func TestSchedulerSkipsARoundOnAFailedRead(t *testing.T) {
	for name, fail := range map[string]func(*fakeMetadata){
		"policy":       func(m *fakeMetadata) { m.getConfigErr = errors.New("unavailable") },
		"data servers": func(m *fakeMetadata) { m.listDataServerErr = errors.New("unavailable") },
		"namespaces":   func(m *fakeMetadata) { m.listNamespaceErr = errors.New("unavailable") },
		"statuses":     func(m *fakeMetadata) { m.listNamespaceStatusErr = errors.New("unavailable") },
	} {
		t.Run(name, func(t *testing.T) {
			metadata := newFakeMetadata(t)
			fail(metadata)
			metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
			s := start(t, metadata)

			assert.Never(t, func() bool { return len(s.GetActions()) > 0 }, 200*time.Millisecond, 10*time.Millisecond)
		})
	}
}

func TestSchedulerWaitsForTheOpsOfARound(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "10ms"}
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	s := start(t, metadata)
	change := <-s.GetActions()

	assert.Never(t, func() bool { return len(s.GetActions()) > 0 }, 200*time.Millisecond, 10*time.Millisecond)

	// A failed op is planned again in the next round
	change.Error(errors.New("not ready"))
	again := (<-s.GetActions()).(*action.ChangeEnsembleAction)
	assert.Equal(t, int64(1), again.Shard)
}

func TestSchedulerRunsTheNextRoundRightAwayAfterItsActionsSucceed(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "1h"}
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	s := start(t, metadata)
	change := (<-s.GetActions()).(*action.ChangeEnsembleAction)

	metadata.setShards("a", map[int64]*proto.ShardMetadata{
		1: {Ensemble: identities(change.To.GetInternal(), "n0")},
		2: {Ensemble: identities("r", "n0")},
	})
	change.Done(nil)

	select {
	case ac := <-s.GetActions():
		assert.Equal(t, int64(2), ac.GetShard())
	case <-time.After(5 * time.Second):
		assert.Fail(t, "the next round waits the interval")
	}
}

func TestSchedulerWaitsTheIntervalAfterAFailedAction(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "1h"}
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r", "n0")}})
	s := start(t, metadata)

	(<-s.GetActions()).Error(errors.New("not ready"))

	assert.Never(t, func() bool { return len(s.GetActions()) > 0 }, 200*time.Millisecond, 10*time.Millisecond)
}

func TestSchedulerSettleReportsWhetherEveryActionSucceeded(t *testing.T) {
	s := &scheduler{logger: slog.Default(), collector: collector.New(t.Context(), nil, nil, nil),
		splits: metric.NewCounter("test_splits", "Splits", "count", map[string]any{})}
	split := action.NewSplitAction("a", 1)
	failed := action.NewChangeEnsembleAction(2, nil, nil)

	assert.True(t, s.settle([]model.Result{{Action: split}}, time.Now()))
	assert.False(t, s.settle([]model.Result{{Action: split}, {Action: failed, Err: errors.New("not ready")}}, time.Now()))
}

func TestSchedulerLogsBlockedReplicas(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.setShards("a", map[int64]*proto.ShardMetadata{1: {Ensemble: identities("r")}})
	s := start(t, metadata)

	assert.Never(t, func() bool { return len(s.GetActions()) > 0 }, 200*time.Millisecond, 10*time.Millisecond)
}

func TestSchedulerPlansAroundADataServerDown(t *testing.T) {
	metadata := newFakeMetadata(t)
	metadata.config.LoadBalancer = &proto.LoadBalancer{ScheduleInterval: "10ms"}
	// Two replicas to drain from r, while n1 is down: the plan moves them onto
	// the data servers that are running
	metadata.setShards("a", map[int64]*proto.ShardMetadata{
		1: {Ensemble: identities("r", "n0")},
		2: {Ensemble: identities("r", "n2")},
	})
	s := New(t.Context(), metadata, func(name string) bool { return name != "n1" }, nil)
	t.Cleanup(func() { assert.NoError(t, s.Close()) })

	targets := map[int64]string{}
	for range 2 {
		select {
		case ac := <-s.GetActions():
			targets[ac.GetShard()] = ac.(*action.ChangeEnsembleAction).To.GetInternal()
		case <-time.After(5 * time.Second):
			require.Fail(t, "a data server down holds the plan")
		}
	}
	assert.Equal(t, map[int64]string{1: "n2", 2: "n0"}, targets)
}

func start(t *testing.T, metadata *fakeMetadata) Scheduler {
	t.Helper()
	s := New(t.Context(), metadata, running, nil)
	t.Cleanup(func() { assert.NoError(t, s.Close()) })
	return s
}

// running reports every data server running.
func running(string) bool {
	return true
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
