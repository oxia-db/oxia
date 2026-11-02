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
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
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
	coordscheduler "github.com/oxia-db/oxia/oxiad/coordinator/scheduler"
)

var (
	errUnexpected = errors.New("unexpected action")
	errInjected   = errors.New("injected failure")
)

const (
	zoneLabel = "zone"
	// quietPeriod is how long the scheduler sends no action before a
	// simulation counts it as done: many rounds of its 10ms interval.
	quietPeriod = 500 * time.Millisecond
)

// simulation runs the scheduler on a cluster that lives in memory. It applies
// each action to the shards as the shard controllers would, and completes it
// right away, so that a test checks the decisions of the scheduler alone:
//
//   - a change of the ensemble replaces its member, and the leader stays if it
//     is still a member, else the first member leads;
//   - a leader transfer makes the preferred leader lead.
//
// With a failure rate, it fails that share of the actions instead, drawn from
// a seeded source: a change of the ensemble leaves the shard as it was, and a
// leader transfer either fails, or elects another member, which the scheduler
// counts as a failure.
//
// A data server removed from the configuration keeps running, and the
// scheduler drains it. After each action, it checks that every ensemble has a
// member in each zone and that the leader is one of them. It serves the reads of the metadata that
// the scheduler makes; the other methods of the interface panic.
type simulation struct {
	coordmetadata.Metadata

	t *testing.T

	mu          sync.Mutex
	config      *proto.ClusterConfiguration
	dataServers map[string]*proto.DataServer
	// draining holds the data servers removed from the configuration: they
	// still run, until the scheduler moves their replicas off.
	draining    map[string]*proto.DataServer
	namespaces  map[string]*proto.Namespace
	shards      map[string]map[int64]*proto.ShardMetadata
	configCache *cache.Cache[provider.Versioned[*proto.ClusterConfiguration]]
	// lastAction is the time of the last action, or of the last change of
	// the cluster.
	lastAction time.Time
	// movedTo counts the replicas that the changes of ensemble moved onto each
	// data server, and transfers the leader transfers, since the last change
	// of the cluster.
	movedTo   map[string]int
	transfers int
	// failureRate is the share of the actions that fail, drawn from random;
	// failures counts them.
	failureRate float64
	random      *rand.Rand
	failures    int
}

// newSimulation starts the scheduler on three data servers, one in each zone,
// with the namespaces a, b and c of 64, 32 and 3 shards, each of 3 replicas
// with an anti-affinity on the zone. Every shard has a replica on each data
// server, and a member drawn from the seeded source leads it. failureRate of
// the actions fail.
func newSimulation(t *testing.T, failureRate float64) *simulation {
	t.Helper()
	s := &simulation{
		failureRate: failureRate,
		random:      rand.New(rand.NewPCG(1, 1)),
		t:           t,
		config:      &proto.ClusterConfiguration{LoadBalancer: &proto.LoadBalancer{ScheduleInterval: "10ms"}},
		dataServers: make(map[string]*proto.DataServer),
		draining:    make(map[string]*proto.DataServer),
		namespaces:  make(map[string]*proto.Namespace),
		shards:      make(map[string]map[int64]*proto.ShardMetadata),
		lastAction:  time.Now(),
		movedTo:     make(map[string]int),
	}
	s.addDataServer("ds-0", "zone-1")
	s.addDataServer("ds-1", "zone-2")
	s.addDataServer("ds-2", "zone-3")
	id := int64(0)
	for _, ns := range []struct {
		name   string
		shards int
	}{{"a", 64}, {"b", 32}, {"c", 3}} {
		s.namespaces[ns.name] = &proto.Namespace{Name: ns.name, InitialShardCount: uint32(ns.shards),
			ReplicationFactor: 3, AntiAffinities: []*proto.AntiAffinity{{Labels: []string{zoneLabel}, Mode: "strict"}}}
		s.shards[ns.name] = make(map[int64]*proto.ShardMetadata)
		for range ns.shards {
			ensemble := []*proto.DataServerIdentity{s.identity("ds-0"), s.identity("ds-1"), s.identity("ds-2")}
			s.shards[ns.name][id] = &proto.ShardMetadata{
				Status:   proto.ShardStatusSteadyState,
				Ensemble: ensemble,
				Leader:   ensemble[s.random.IntN(len(ensemble))].CloneVT(),
			}
			id++
		}
	}

	t.Logf("initially:\n%s", s.distribution())

	ctx, cancel := context.WithCancel(t.Context())
	s.configCache = cache.New(ctx, func(context.Context) (*provider.Versioned[*proto.ClusterConfiguration], error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return &provider.Versioned[*proto.ClusterConfiguration]{Value: s.config}, nil
	}, nil)
	sched := coordscheduler.New(ctx, s, func(name string) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.dataServer(name) != nil
	}, nil)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case ac := <-sched.GetActions():
				s.apply(ac)
			case <-ctx.Done():
				return
			}
		}
	})
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, sched.Close())
		wg.Wait()
		_ = s.configCache.Close()
	})
	return s
}

// addDataServer adds a data server in zone: the next round sees it, and the
// quiet period and the counts of the moves start over.
func (s *simulation) addDataServer(name string, zone string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAction = time.Now()
	s.resetCounts()
	s.dataServers[name] = &proto.DataServer{
		Identity: &proto.DataServerIdentity{Name: &name, Public: name + ":9190", Internal: name + ":8190"},
		Metadata: &proto.DataServerMetadata{Labels: map[string]string{zoneLabel: zone}},
	}
}

// removeDataServer removes a data server from the configuration: it keeps
// running while the scheduler drains it, and the quiet period and the counts
// of the moves start over.
func (s *simulation) removeDataServer(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAction = time.Now()
	s.resetCounts()
	s.draining[name] = s.dataServers[name]
	delete(s.dataServers, name)
}

// resetCounts starts the counts of the moves over; the caller holds mu.
func (s *simulation) resetCounts() {
	s.movedTo, s.transfers, s.failures = make(map[string]int), 0, 0
}

// dataServer returns a data server of the configuration, or one draining, or
// nil; the caller holds mu.
func (s *simulation) dataServer(name string) *proto.DataServer {
	if dataServer, ok := s.dataServers[name]; ok {
		return dataServer
	}
	return s.draining[name]
}

// identity returns a copy of the identity of a data server.
func (s *simulation) identity(name string) *proto.DataServerIdentity {
	return s.dataServers[name].GetIdentity().CloneVT()
}

// waitBalanced waits until the scheduler sends no more action, and checks that
// the cluster is then balanced.
func (s *simulation) waitBalanced() {
	s.t.Helper()
	require.Eventually(s.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return time.Since(s.lastAction) > quietPeriod
	}, time.Minute, 50*time.Millisecond, "the scheduler doesn't settle")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.t.Logf("after %d replica moves, %d leader transfers and %d failures:\n%s", sum(s.movedTo), s.transfers,
		s.failures, s.distribution())
	require.NoError(s.t, s.balanced())
}

// distribution returns a table of the replicas and the leaders of each data
// server, by namespace and in total.
func (s *simulation) distribution() string {
	namespaces := slices.Sorted(maps.Keys(s.shards))
	replicas, leaders := make(map[string]map[string]int), make(map[string]map[string]int)
	for _, name := range append(slices.Clone(namespaces), "total") {
		replicas[name], leaders[name] = make(map[string]int), make(map[string]int)
	}
	for _, name := range namespaces {
		for _, shard := range s.shards[name] {
			for _, ns := range []string{name, "total"} {
				leaders[ns][shard.GetLeader().GetNameOrDefault()]++
				for _, member := range shard.GetEnsemble() {
					replicas[ns][member.GetNameOrDefault()]++
				}
			}
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%-6s %-7s", "server", "zone")
	for _, name := range append(slices.Clone(namespaces), "total") {
		fmt.Fprintf(&b, " | %-5s replicas/leaders", name)
	}
	names := slices.SortedFunc(maps.Keys(s.dataServers), func(a, b string) int {
		return cmp.Or(strings.Compare(zoneOf(s.dataServers[a]), zoneOf(s.dataServers[b])), strings.Compare(a, b))
	})
	for _, server := range names {
		fmt.Fprintf(&b, "\n%-6s %-7s", server, zoneOf(s.dataServers[server]))
		for _, name := range append(slices.Clone(namespaces), "total") {
			fmt.Fprintf(&b, " | %14d / %-7d", replicas[name][server], leaders[name][server])
		}
	}
	return b.String()
}

func sum(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

// balanced returns why the cluster isn't balanced, or nil: in each namespace,
// and in all of them, the leaders of two data servers differ by at most one,
// and so do the replicas of two data servers in the same zone. The
// anti-affinity keeps a replica in its zone, so the replicas of two zones
// can't be compared.
func (s *simulation) balanced() error {
	totalLeaders := make(map[string]int)
	totalReplicas := make(map[string]int)
	for _, name := range slices.Sorted(maps.Keys(s.shards)) {
		leaders := make(map[string]int)
		replicas := make(map[string]int)
		for _, shard := range s.shards[name] {
			leaders[shard.GetLeader().GetNameOrDefault()]++
			totalLeaders[shard.GetLeader().GetNameOrDefault()]++
			for _, member := range shard.GetEnsemble() {
				replicas[member.GetNameOrDefault()]++
				totalReplicas[member.GetNameOrDefault()]++
			}
		}
		if err := s.even(fmt.Sprintf("leaders of namespace %s", name), leaders, anyDataServer); err != nil {
			return err
		}
		if err := s.even(fmt.Sprintf("replicas of namespace %s", name), replicas, zoneOf); err != nil {
			return err
		}
	}
	if err := s.even("leaders", totalLeaders, anyDataServer); err != nil {
		return err
	}
	return s.even("replicas", totalReplicas, zoneOf)
}

// anyDataServer puts every data server in the same group.
func anyDataServer(*proto.DataServer) string {
	return ""
}

// zoneOf puts the data servers of a zone in the same group.
func zoneOf(dataServer *proto.DataServer) string {
	return dataServer.GetMetadata().GetLabels()[zoneLabel]
}

// even returns an error if the counts of two data servers of the same group
// differ by more than one.
func (s *simulation) even(what string, counts map[string]int, group func(*proto.DataServer) string) error {
	groups := make(map[string][]string)
	for name, dataServer := range s.dataServers {
		groups[group(dataServer)] = append(groups[group(dataServer)], name)
	}
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		names := groups[group]
		low, high := counts[names[0]], counts[names[0]]
		for _, name := range names {
			low, high = min(low, counts[name]), max(high, counts[name])
		}
		if high-low > 1 {
			byName := make(map[string]int, len(names))
			for _, name := range names {
				byName[name] = counts[name]
			}
			return fmt.Errorf("the %s aren't balanced: %v", what, byName)
		}
	}
	return nil
}

// requireDrained checks that the removed data servers hold no replica. The
// drain may also move a few replicas between the data servers that stay, to
// balance the namespaces, e.g. after a failed move.
func (s *simulation) requireDrained(removed ...string) {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, shards := range s.shards {
		for id, shard := range shards {
			for _, member := range shard.GetEnsemble() {
				assert.NotContains(s.t, removed, member.GetNameOrDefault(), "shard %d still has a replica on it", id)
			}
		}
	}
}

// requireMovedOnlyTo checks that, since the last change of the cluster, the
// changes of ensemble moved replicas onto the data servers just added only, as
// many as they now hold: no replica moved between the other data servers, or
// twice.
func (s *simulation) requireMovedOnlyTo(added ...string) {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	held := make(map[string]int)
	for _, shards := range s.shards {
		for _, shard := range shards {
			for _, member := range shard.GetEnsemble() {
				if slices.Contains(added, member.GetNameOrDefault()) {
					held[member.GetNameOrDefault()]++
				}
			}
		}
	}
	assert.Equal(s.t, held, s.movedTo, "the replicas moved onto each data server")
}

// apply applies ac to its shard, and completes it. It runs on the goroutine
// of the model, so it reports a wrong action with assert, and fails it.
func (s *simulation) apply(ac action.Action) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAction = time.Now()
	shard := s.shard(ac.GetShard())
	if !assert.NotNil(s.t, shard, "shard %d is unknown", ac.GetShard()) {
		ac.Error(errUnexpected)
		return
	}
	member := func(name string) int {
		return slices.IndexFunc(shard.Ensemble, func(member *proto.DataServerIdentity) bool {
			return member.GetNameOrDefault() == name
		})
	}
	fail := s.random.Float64() < s.failureRate
	switch ac := ac.(type) {
	case *action.ChangeEnsembleAction:
		from, to := ac.From.GetNameOrDefault(), ac.To.GetNameOrDefault()
		i := member(from)
		if !assert.GreaterOrEqual(s.t, i, 0, "shard %d has no member %q", ac.Shard, from) {
			ac.Error(errUnexpected)
			return
		}
		if fail {
			s.failures++
			ac.Error(errInjected)
			return
		}
		shard.Ensemble[i] = s.identity(to)
		if shard.GetLeader().GetNameOrDefault() == from {
			shard.Leader = shard.Ensemble[0].CloneVT()
		}
		s.t.Logf("shard %d: replace %s with %s", ac.Shard, from, to)
		s.movedTo[to]++
		s.check(ac.Shard, shard)
		ac.Done(nil)
	case *action.ElectionAction:
		if !assert.GreaterOrEqual(s.t, member(ac.PreferredLeader), 0, "shard %d moves its leader to %q, not a member",
			ac.Shard, ac.PreferredLeader) {
			ac.Error(errUnexpected)
			return
		}
		leader := ac.PreferredLeader
		if fail {
			s.failures++
			if s.random.IntN(2) == 0 {
				ac.Error(errInjected)
				return
			}
			// The election elects another member, e.g. one further ahead
			others := slices.DeleteFunc(slices.Clone(shard.Ensemble), func(member *proto.DataServerIdentity) bool {
				return member.GetNameOrDefault() == ac.PreferredLeader
			})
			leader = others[s.random.IntN(len(others))].GetNameOrDefault()
		}
		shard.Leader = s.identity(leader)
		s.check(ac.Shard, shard)
		if !fail {
			s.transfers++
		}
		ac.Done(leader)
	default:
		assert.Failf(s.t, "unexpected action", "%v", ac)
		ac.Error(errUnexpected)
	}
}

// check checks that the ensemble of shard has a member in each zone, and that
// its leader is one of them.
func (s *simulation) check(id int64, shard *proto.ShardMetadata) {
	zones := make(map[string]bool)
	members := make([]string, 0, len(shard.GetEnsemble()))
	leads := false
	for _, member := range shard.GetEnsemble() {
		zone := zoneOf(s.dataServer(member.GetNameOrDefault()))
		zones[zone] = true
		members = append(members, member.GetNameOrDefault()+"@"+zone)
		leads = leads || member.GetNameOrDefault() == shard.GetLeader().GetNameOrDefault()
	}
	assert.Len(s.t, zones, 3, "the members of shard %d share a zone: %v", id, members)
	assert.True(s.t, leads, "the leader of shard %d isn't a member", id)
}

// shard returns the shard with id, or nil; the caller holds mu.
func (s *simulation) shard(id int64) *proto.ShardMetadata {
	for _, shards := range s.shards {
		if shard, ok := shards[id]; ok {
			return shard
		}
	}
	return nil
}

func (s *simulation) SubscribeConfig() *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]] {
	return s.configCache.Subscribe()
}

func (s *simulation) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return commonobject.Borrow(s.config.CloneVT()), nil
}

func (s *simulation) ListDataServer() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.DataServer], len(s.dataServers))
	for name, dataServer := range s.dataServers {
		result[name] = commonobject.Borrow(dataServer.CloneVT())
	}
	return result, nil
}

// ListDataServerStatus returns the status of each data server, with its
// metadata, as the reconciler stores it: also for one draining, which the
// configuration no longer has.
func (s *simulation) ListDataServerStatus() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.DataServerStatus], len(s.dataServers)+len(s.draining))
	for state, dataServers := range map[proto.DataServerState]map[string]*proto.DataServer{
		proto.DataServerState_DATA_SERVER_STATE_RUNNING:  s.dataServers,
		proto.DataServerState_DATA_SERVER_STATE_DRAINING: s.draining,
	} {
		for name, dataServer := range dataServers {
			result[name] = commonobject.Borrow(&proto.DataServerStatus{State: state,
				Metadata: dataServer.GetMetadata().CloneVT()})
		}
	}
	return result, nil
}

func (s *simulation) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.Namespace], len(s.namespaces))
	for name, namespace := range s.namespaces {
		result[name] = commonobject.Borrow(namespace.CloneVT())
	}
	return result, nil
}

// ListNamespaceStatus returns a copy of the shards: the actions change them
// while the scheduler reads its snapshot.
func (s *simulation) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(s.shards))
	for name, shards := range s.shards {
		status := &proto.NamespaceStatus{ReplicationFactor: 3, Shards: make(map[int64]*proto.ShardMetadata)}
		for id, shard := range shards {
			status.Shards[id] = shard.CloneVT()
		}
		result[name] = commonobject.Borrow(status)
	}
	return result, nil
}
