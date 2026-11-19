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

package internal

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{hashRange(1, 2), hashRange(3, 6), false},
		{hashRange(1, 4), hashRange(3, 6), true},
		{hashRange(4, 5), hashRange(3, 6), true},
		{hashRange(5, 8), hashRange(3, 6), true},
		{hashRange(7, 8), hashRange(3, 6), false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}

// numericKeyStrategy hashes a key, a number, to itself.
type numericKeyStrategy struct{}

func (numericKeyStrategy) HashCode(key string) uint32 {
	code, err := strconv.ParseUint(key, 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(code)
}

func (n numericKeyStrategy) Get(key string) func(Shard) bool {
	code := n.HashCode(key)
	return func(shard Shard) bool {
		return shard.HashRange.MinInclusive <= code && code <= shard.HashRange.MaxInclusive
	}
}

// newTestShardManager returns a shardManagerImpl that routes numeric keys,
// with no connection to a service.
func newTestShardManager() *shardManagerImpl {
	return &shardManagerImpl{
		shardStrategy: numericKeyStrategy{},
		shards:        make(map[int64]Shard),
		successors:    make(map[int64][]Shard),
		changed:       make(chan struct{}),
		updatedWg:     concurrent.NewWaitGroup(1),
		logger:        slog.Default(),
	}
}

func TestShardManagerSuccessors(t *testing.T) {
	var replaced []map[int64][]int64
	s := &shardManagerImpl{
		shardStrategy: numericKeyStrategy{},
		shards:        make(map[int64]Shard),
		successors:    make(map[int64][]Shard),
		changed:       make(chan struct{}),
		updatedWg:     concurrent.NewWaitGroup(1),
		logger:        slog.Default(),
	}
	s.onShardsReplaced = func(r map[int64][]int64) {
		// The new shard map is not visible yet
		assert.False(t, s.TryRLock())
		replaced = append(replaced, r)
	}
	s.update([]Shard{
		{Id: 0, HashRange: hashRange(0, 99)},
		{Id: 1, HashRange: hashRange(100, 199)},
	})
	assert.Nil(t, s.GetSuccessors(0))
	assert.Empty(t, replaced)

	// Shard 0 is split into shards 2 and 3
	s.update([]Shard{
		{Id: 1, HashRange: hashRange(100, 199)},
		{Id: 2, HashRange: hashRange(0, 49)},
		{Id: 3, HashRange: hashRange(50, 99)},
	})
	assert.ElementsMatch(t, []int64{2, 3}, s.GetSuccessors(0))
	assert.Nil(t, s.GetSuccessors(1))
	assertSuccessor(t, s, 0, "10", 2)
	assertSuccessor(t, s, 0, "60", 3)
	require.Len(t, replaced, 1)
	assert.ElementsMatch(t, []int64{2, 3}, replaced[0][0])

	// Shard 3 is split too, before the calls pending on shard 0 are rerouted:
	// they still go to shard 3 first, which reroutes them in turn, so they
	// keep their order
	s.update([]Shard{
		{Id: 1, HashRange: hashRange(100, 199)},
		{Id: 2, HashRange: hashRange(0, 49)},
		{Id: 4, HashRange: hashRange(50, 74)},
		{Id: 5, HashRange: hashRange(75, 99)},
	})
	assert.ElementsMatch(t, []int64{2, 3}, s.GetSuccessors(0))
	assert.ElementsMatch(t, []int64{4, 5}, s.GetSuccessors(3))
	assertSuccessor(t, s, 0, "60", 3)
	assertSuccessor(t, s, 3, "60", 4)
	assertSuccessor(t, s, 3, "80", 5)
	_, ok := s.GetSuccessor(1, "150")
	assert.False(t, ok)
	require.Len(t, replaced, 2)
	assert.ElementsMatch(t, []int64{4, 5}, replaced[1][3])
}

func assertSuccessor(t *testing.T, s *shardManagerImpl, shardId int64, key string, expected int64) {
	t.Helper()
	successor, ok := s.GetSuccessor(shardId, key)
	assert.True(t, ok)
	assert.Equal(t, expected, successor)
}

func TestShardManagerChanged(t *testing.T) {
	s := &shardManagerImpl{
		shards:     make(map[int64]Shard),
		successors: make(map[int64][]Shard),
		changed:    make(chan struct{}),
		updatedWg:  concurrent.NewWaitGroup(1),
		logger:     slog.Default(),
	}
	changed := s.Changed()
	select {
	case <-changed:
		assert.Fail(t, "the shard map has not changed")
	default:
	}

	s.update([]Shard{{Id: 0, HashRange: hashRange(0, 99)}})
	select {
	case <-changed:
	default:
		assert.Fail(t, "the shard map has changed")
	}
	assert.NotEqual(t, changed, s.Changed())
}

func TestShardManagerGetPanicsWithoutShardForKey(t *testing.T) {
	s := newTestShardManager()
	s.update([]Shard{{Id: 0, HashRange: hashRange(0, 99)}, {Id: 1, HashRange: hashRange(200, 299)}})
	assert.EqualValues(t, 0, s.Get("10"))
	assert.PanicsWithValue(t, "shard not found", func() { s.Get("150") })
	assert.PanicsWithValue(t, "shard not found", func() { s.Get("300") })
}

func TestShardManagerGetFindsTheShardOfEveryKey(t *testing.T) {
	s := newTestShardManager()
	// The updates come in any order, and a split replaces a shard by two.
	s.update([]Shard{
		{Id: 2, HashRange: hashRange(200, 299)},
		{Id: 0, HashRange: hashRange(0, 99)},
		{Id: 1, HashRange: hashRange(100, 199)},
	})
	s.update([]Shard{
		{Id: 0, HashRange: hashRange(0, 99)},
		{Id: 2, HashRange: hashRange(200, 299)},
		{Id: 3, HashRange: hashRange(100, 149)},
		{Id: 4, HashRange: hashRange(150, 199)},
	})
	for key, expected := range map[string]int64{
		"0": 0, "99": 0,
		"100": 3, "149": 3,
		"150": 4, "199": 4,
		"200": 2, "299": 2,
	} {
		assert.Equal(t, expected, s.Get(key), "key %s", key)
	}
}

func BenchmarkShardManagerGet(b *testing.B) {
	const shards = 64
	s := newTestShardManager()
	updates := make([]Shard, shards)
	step := uint32(math.MaxUint32 / shards)
	for i := range updates {
		maxInclusive := uint32(i+1)*step - 1
		if i == shards-1 {
			maxInclusive = math.MaxUint32
		}
		updates[i] = Shard{Id: int64(i), HashRange: hashRange(uint32(i)*step, maxInclusive)}
	}
	s.update(updates)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = strconv.FormatUint(uint64(i)*uint64(step/1024)*uint64(shards), 10)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(keys[i%len(keys)])
	}
}

// The requests for a shard missing from the shard map go to the service address.
func TestShardManagerLeaderOfUnknownShard(t *testing.T) {
	s := &shardManagerImpl{
		serviceAddress: "service:6648",
		shards:         map[int64]Shard{0: {Id: 0, Leader: "leader:6648"}},
	}
	assert.Equal(t, "leader:6648", s.Leader(0))
	assert.Equal(t, "service:6648", s.Leader(1))
}

func TestShardManagerReceiveFailsOnInvalidAssignments(t *testing.T) {
	for name, tt := range map[string]struct {
		assignments *proto.NamespaceShardsAssignment
		namespace   string
		err         string
	}{
		"other-namespace": {
			assignments: &proto.NamespaceShardsAssignment{},
			namespace:   "other",
			err:         "namespace not found in shards assignments",
		},
		"unsupported-boundaries": {
			assignments: &proto.NamespaceShardsAssignment{Assignments: []*proto.ShardAssignment{{Shard: 5}}},
			namespace:   "default",
			err:         "shard 5 has an unsupported hash range boundary",
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := &shardManagerImpl{
				rpcProvider: &singleAssignmentsRpcProvider{assignments: &proto.ShardAssignments{
					Namespaces: map[string]*proto.NamespaceShardsAssignment{tt.namespace: tt.assignments},
				}},
				namespace:  "default",
				shards:     make(map[int64]Shard),
				successors: make(map[int64][]Shard),
				changed:    make(chan struct{}),
				updatedWg:  concurrent.NewWaitGroup(1),
				logger:     slog.Default(),
			}
			s.ctx, s.cancel = context.WithCancel(t.Context())
			defer s.cancel()

			err := s.receive(&backoff.ZeroBackOff{})
			assert.ErrorContains(t, err, tt.err)
			assert.Empty(t, s.GetAll())
		})
	}
}

// Sends the shard assignments a number of times, then fails with err.
type failingAssignmentsStream struct {
	grpc.ClientStream
	namespace   string
	assignments int
	err         error
}

func (s *failingAssignmentsStream) Recv() (*proto.ShardAssignments, error) {
	if s.assignments == 0 {
		return nil, s.err
	}
	s.assignments--
	return &proto.ShardAssignments{Namespaces: map[string]*proto.NamespaceShardsAssignment{
		s.namespace: {Assignments: []*proto.ShardAssignment{{
			ShardBoundaries: &proto.ShardAssignment_Int32HashRange{Int32HashRange: &proto.Int32HashRange{}},
		}}},
	}}, nil
}

type failingAssignmentsRpcProvider struct {
	RpcProvider
	assignments int
	err         error
}

func (p *failingAssignmentsRpcProvider) GetShardAssignments(_ context.Context, _ string,
	request *proto.ShardAssignmentsRequest) (proto.OxiaClient_GetShardAssignmentsClient, error) {
	return &failingAssignmentsStream{namespace: request.Namespace, assignments: p.assignments, err: p.err}, nil
}

// Signals the warnings, such as the one logged before retrying to receive the
// shard assignments.
type warningsHandler struct {
	slog.Handler
	warnings chan struct{}
}

func (h *warningsHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level == slog.LevelWarn {
		select {
		case h.warnings <- struct{}{}:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

// Closing the shard manager interrupts the wait before it retries to receive
// the assignments. It must stop without reporting a failure: once assignments
// were received, reporting it blocks forever.
func TestShardManagerCloseWhileWaitingToRetry(t *testing.T) {
	logs := &warningsHandler{Handler: slog.Default().Handler(), warnings: make(chan struct{}, 1)}
	s := &shardManagerImpl{
		rpcProvider: &failingAssignmentsRpcProvider{assignments: 1, err: status.Error(codes.Unavailable, "connection reset")},
		namespace:   "default",
		shards:      make(map[int64]Shard),
		successors:  make(map[int64][]Shard),
		changed:     make(chan struct{}),
		updatedWg:   concurrent.NewWaitGroup(1),
		logger:      slog.New(logs),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		s.receiveWithRecovery()
		close(stopped)
	}()

	select {
	case <-logs.warnings:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the failed stream was not retried")
	}

	assert.NoError(t, s.Close())
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the shard manager kept receiving the assignments after it was closed")
	}
}

// Sends one set of assignments, then waits for the shard manager to close.
type singleAssignmentsStream struct {
	grpc.ClientStream
	ctx         context.Context
	assignments *proto.ShardAssignments
}

func (s *singleAssignmentsStream) Recv() (*proto.ShardAssignments, error) {
	if assignments := s.assignments; assignments != nil {
		s.assignments = nil
		return assignments, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// Sends the given assignments if set, else one shard with the key sorting.
type singleAssignmentsRpcProvider struct {
	RpcProvider
	keySorting  proto.KeySorting
	assignments *proto.ShardAssignments
}

func (p *singleAssignmentsRpcProvider) GetShardAssignments(ctx context.Context, _ string,
	request *proto.ShardAssignmentsRequest) (proto.OxiaClient_GetShardAssignmentsClient, error) {
	if p.assignments != nil {
		return &singleAssignmentsStream{ctx: ctx, assignments: p.assignments}, nil
	}
	return &singleAssignmentsStream{ctx: ctx, assignments: &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			request.Namespace: {
				Assignments: []*proto.ShardAssignment{{
					ShardBoundaries: &proto.ShardAssignment_Int32HashRange{Int32HashRange: &proto.Int32HashRange{}},
				}},
				KeySorting: p.keySorting,
			},
		},
	}}, nil
}

// The shard manager reports the key sorting of the namespace that the server
// sends with the assignments, or none when an older server does not send it.
func TestShardManagerKeySorting(t *testing.T) {
	for _, keySorting := range []proto.KeySorting{
		proto.KeySorting_KEY_SORTING_UNKNOWN,
		proto.KeySorting_KEY_SORTING_NATURAL,
		proto.KeySorting_KEY_SORTING_HIERARCHICAL,
	} {
		provider := &singleAssignmentsRpcProvider{keySorting: keySorting}
		sm, err := NewShardManager(NewShardStrategy(), provider, "localhost:6648", "default", 10*time.Second, nil)
		require.NoError(t, err)
		assert.Equal(t, keySorting, sm.KeySorting())
		assert.NoError(t, sm.Close())
	}
}

// Waits for the shard manager to cancel the request of the assignments.
type pendingAssignmentsRpcProvider struct {
	RpcProvider
	canceled chan struct{}
}

func (p *pendingAssignmentsRpcProvider) GetShardAssignments(ctx context.Context, _ string,
	_ *proto.ShardAssignmentsRequest) (proto.OxiaClient_GetShardAssignmentsClient, error) {
	<-ctx.Done()
	close(p.canceled)
	return nil, ctx.Err()
}

// A shard manager fails to start right away on an error that a retry can't fix,
// and reports that error rather than a timeout.
func TestNewShardManagerFailsOnPermanentErrors(t *testing.T) {
	for name, permanentErr := range map[string]error{
		"unauthenticated":     status.Error(codes.Unauthenticated, "token expired"),
		"permission-denied":   status.Error(codes.PermissionDenied, "oxia: unexpected authority"),
		"namespace-not-found": constant.ErrNamespaceNotFound,
	} {
		t.Run(name, func(t *testing.T) {
			provider := &failingAssignmentsRpcProvider{err: permanentErr}
			_, err := NewShardManager(NewShardStrategy(), provider, "localhost:6648", "default", 10*time.Second, nil)
			assert.ErrorIs(t, err, permanentErr)
			assert.NotErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}

// A shard manager that does not receive the initial assignments in time fails
// to start, and must stop requesting them.
func TestNewShardManagerStopsReceivingOnTimeout(t *testing.T) {
	provider := &pendingAssignmentsRpcProvider{canceled: make(chan struct{})}
	_, err := NewShardManager(NewShardStrategy(), provider, "localhost:6648", "default", 100*time.Millisecond, nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-provider.canceled:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the shard manager kept requesting the assignments after it failed to start")
	}
}

// The errors that fail the creation of the shard manager are retried once it
// has received the initial assignments.
func TestShardManagerRetriesPermanentErrorsAfterStart(t *testing.T) {
	for name, err := range map[string]error{
		"unauthenticated":     status.Error(codes.Unauthenticated, "token expired"),
		"permission-denied":   status.Error(codes.PermissionDenied, "oxia: unexpected authority"),
		"namespace-not-found": constant.ErrNamespaceNotFound,
	} {
		t.Run(name, func(t *testing.T) {
			logs := &warningsHandler{Handler: slog.Default().Handler(), warnings: make(chan struct{}, 1)}
			s := &shardManagerImpl{
				// The initial assignments and an update, then the error
				rpcProvider: &failingAssignmentsRpcProvider{assignments: 2, err: err},
				namespace:   "default",
				shards:      make(map[int64]Shard),
				successors:  make(map[int64][]Shard),
				changed:     make(chan struct{}),
				updatedWg:   concurrent.NewWaitGroup(1),
				logger:      slog.New(logs),
			}
			s.ctx, s.cancel = context.WithCancel(context.Background())
			stopped := make(chan struct{})
			go func() {
				s.receiveWithRecovery()
				close(stopped)
			}()

			select {
			case <-logs.warnings:
			case <-time.After(10 * time.Second):
				require.FailNow(t, "the error was not retried")
			}

			assert.NoError(t, s.Close())
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				require.FailNow(t, "the shard manager kept receiving the assignments after it was closed")
			}
		})
	}
}
