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

package oxia

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxia/internal"
)

func TestNotificationsClose(t *testing.T) {
	count := 0
	ctx, cancel := context.WithCancel(context.Background())

	nm := &notifications{
		cancel: func() {
			count++
		},
		ctxMultiplexChanClosed: ctx,
		multiplexCh:            make(chan *Notification, 100),
	}
	nm.multiplexCh <- &Notification{
		Key: "key1",
	}
	nm.multiplexCh <- &Notification{
		Key: "key2",
	}
	close(nm.multiplexCh)

	cancel()
	err := nm.Close()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	n, ok := <-nm.multiplexCh
	assert.Equal(t, false, ok)
	assert.Nil(t, n)
}

type notificationsTestShardManager struct {
	internal.ShardManager
	sync.Mutex
	shards     []int64
	successors map[int64][]int64
	changed    chan struct{}
}

func (m *notificationsTestShardManager) GetAll() []int64 {
	m.Lock()
	defer m.Unlock()
	return slices.Clone(m.shards)
}

func (*notificationsTestShardManager) Leader(int64) string { return "server-1" }

func (m *notificationsTestShardManager) Exists(shard int64) bool {
	m.Lock()
	defer m.Unlock()
	return slices.Contains(m.shards, shard)
}

func (m *notificationsTestShardManager) GetSuccessors(shard int64) []int64 {
	m.Lock()
	defer m.Unlock()
	return m.successors[shard]
}

func (m *notificationsTestShardManager) Changed() <-chan struct{} {
	m.Lock()
	defer m.Unlock()
	if m.changed == nil {
		m.changed = make(chan struct{})
	}
	return m.changed
}

// split replaces the shard with its successors in the shard map.
func (m *notificationsTestShardManager) split(shard int64, successors ...int64) {
	m.Lock()
	defer m.Unlock()
	m.shards = append(slices.DeleteFunc(m.shards, func(s int64) bool { return s == shard }), successors...)
	if m.successors == nil {
		m.successors = map[int64][]int64{}
	}
	m.successors[shard] = successors
	if m.changed != nil {
		close(m.changed)
	}
	m.changed = make(chan struct{})
}

type notificationsTestStream struct {
	grpc.ClientStream
	ctx     context.Context
	batches chan *proto.NotificationBatch
}

func (s *notificationsTestStream) Recv() (*proto.NotificationBatch, error) {
	select {
	case nb := <-s.batches:
		return nb, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// Returns each batch in turn, then fails every later Recv() with err.
type notificationsTestScriptedStream struct {
	grpc.ClientStream
	batches []*proto.NotificationBatch
	err     error
}

func (s *notificationsTestScriptedStream) Recv() (*proto.NotificationBatch, error) {
	if len(s.batches) == 0 {
		return nil, s.err
	}
	nb := s.batches[0]
	s.batches = s.batches[1:]
	return nb, nil
}

type notificationsTestRpcProvider struct {
	internal.RpcProvider
	failures atomic.Int32 // Number of initial GetNotifications calls that will fail
	err      error
	attempts atomic.Int32
	stream   proto.OxiaClient_GetNotificationsClient // If set, returned instead of a new stream
}

func (p *notificationsTestRpcProvider) GetNotifications(ctx context.Context, _ string,
	_ *proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error) {
	p.attempts.Add(1)
	if p.failures.Add(-1) >= 0 {
		return nil, p.err
	}
	if p.stream != nil {
		return p.stream, nil
	}

	stream := &notificationsTestStream{ctx: ctx, batches: make(chan *proto.NotificationBatch, 1)}
	// The first batch only confirms that the notification cursor is created
	stream.batches <- &proto.NotificationBatch{Offset: 0}
	return stream, nil
}

func TestNotificationsInitRetriesOnRetryableError(t *testing.T) {
	provider := &notificationsTestRpcProvider{err: constant.ErrNotInitialized}
	provider.failures.Store(1)
	shardManager := &notificationsTestShardManager{shards: []int64{0}}

	nm, err := newNotifications(context.Background(), clientOptions{requestTimeout: 10 * time.Second},
		provider, shardManager)
	require.NoError(t, err)
	assert.EqualValues(t, 2, provider.attempts.Load())
	assert.NoError(t, nm.Close())
}

func TestNotificationsInitFailsOnNonRetryableError(t *testing.T) {
	provider := &notificationsTestRpcProvider{err: constant.ErrNotificationsNotEnabled}
	provider.failures.Store(1)
	shardManager := &notificationsTestShardManager{shards: []int64{0}}

	nm, err := newNotifications(context.Background(), clientOptions{requestTimeout: 10 * time.Second},
		provider, shardManager)
	require.Error(t, err)
	assert.Nil(t, nm)
	assert.EqualValues(t, 1, provider.attempts.Load())
}

// Counts the resets of the backoff.
type notificationsTestBackOff struct {
	backoff.BackOff
	resets int
}

func (b *notificationsTestBackOff) Reset() {
	b.resets++
	b.BackOff.Reset()
}

func newTestShardNotificationsManager(stream proto.OxiaClient_GetNotificationsClient) (
	*shardNotificationsManager, *notificationsTestBackOff) {
	bo := &notificationsTestBackOff{BackOff: time2.NewBackOff(context.Background())}
	return &shardNotificationsManager{
		ctx: context.Background(),
		nm: &notifications{
			multiplexCh:  make(chan *Notification, 100),
			shardManager: &notificationsTestShardManager{},
			rpcProvider:  &notificationsTestRpcProvider{stream: stream},
		},
		backoff:            bo,
		lastOffsetReceived: -1,
		log:                slog.Default(),
	}, bo
}

func TestNotificationsRejectionDoesNotResetBackoff(t *testing.T) {
	// The stream is created even when the server rejects the subscription: the
	// rejection is only reported by the first Recv(), so no batch ever arrives.
	snm, bo := newTestShardNotificationsManager(&notificationsTestScriptedStream{
		err: constant.ErrNodeIsNotLeader,
	})

	assert.ErrorIs(t, snm.getNotifications(), constant.ErrNodeIsNotLeader)
	assert.Zero(t, bo.resets)
}

func TestNotificationsReceivedBatchResetsBackoff(t *testing.T) {
	// A batch arrives only once the subscription was accepted, so a stream that
	// delivered one before failing, eg. because the leader stepped down, should
	// be retried quickly.
	snm, bo := newTestShardNotificationsManager(&notificationsTestScriptedStream{
		batches: []*proto.NotificationBatch{{Offset: 5}},
		err:     constant.ErrResourceUnavailable,
	})
	snm.initialized = true

	assert.ErrorIs(t, snm.getNotifications(), constant.ErrResourceUnavailable)
	assert.Equal(t, 1, bo.resets)
	assert.EqualValues(t, 5, snm.lastOffsetReceived)
}

// notificationsTestServer serves the subscriptions of a client: it confirms
// each one at the offset it starts from, or at the shard's commit offset 0, and
// lets the test send the next batches, or fail the stream.
type notificationsTestServer struct {
	internal.RpcProvider
	sync.Mutex
	// The offset that the next subscription to a shard is confirmed at, if not
	// the one it starts from
	confirmedOffsets map[int64]int64
	// The error that the subscriptions to a shard fail with
	rejections map[int64]error
	// The shards whose subscriptions the test confirms
	unconfirmed   map[int64]bool
	attempts      map[int64]int
	subscriptions chan *notificationsTestSubscription
}

func newNotificationsTestServer() *notificationsTestServer {
	return &notificationsTestServer{
		confirmedOffsets: map[int64]int64{},
		rejections:       map[int64]error{},
		unconfirmed:      map[int64]bool{},
		attempts:         map[int64]int{},
		subscriptions:    make(chan *notificationsTestSubscription, 100),
	}
}

func (s *notificationsTestServer) GetNotifications(ctx context.Context, _ string,
	req *proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error) {
	s.Lock()
	defer s.Unlock()
	s.attempts[req.Shard]++
	if err := s.rejections[req.Shard]; err != nil {
		return nil, err
	}
	offset, ok := s.confirmedOffsets[req.Shard]
	if !ok {
		offset = req.GetStartOffsetExclusive()
	}
	delete(s.confirmedOffsets, req.Shard)

	subscription := &notificationsTestSubscription{
		ctx:     ctx,
		request: req,
		batches: make(chan *proto.NotificationBatch, 100),
		errs:    make(chan error, 1),
	}
	if !s.unconfirmed[req.Shard] {
		subscription.send(offset)
	}
	s.subscriptions <- subscription
	return subscription, nil
}

// leaveUnconfirmed has the test confirm the subscriptions to the shard.
func (s *notificationsTestServer) leaveUnconfirmed(shard int64) {
	s.Lock()
	defer s.Unlock()
	s.unconfirmed[shard] = true
}

func (s *notificationsTestServer) confirmAt(shard int64, offset int64) {
	s.Lock()
	defer s.Unlock()
	s.confirmedOffsets[shard] = offset
}

func (s *notificationsTestServer) reject(shard int64, err error) {
	s.Lock()
	defer s.Unlock()
	s.rejections[shard] = err
}

func (s *notificationsTestServer) attemptsOn(shard int64) int {
	s.Lock()
	defer s.Unlock()
	return s.attempts[shard]
}

func (s *notificationsTestServer) nextSubscription(t *testing.T) *notificationsTestSubscription {
	t.Helper()
	select {
	case subscription := <-s.subscriptions:
		return subscription
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no subscription")
		return nil
	}
}

type notificationsTestSubscription struct {
	grpc.ClientStream
	ctx     context.Context
	request *proto.NotificationsRequest
	batches chan *proto.NotificationBatch
	errs    chan error
}

func (s *notificationsTestSubscription) Recv() (*proto.NotificationBatch, error) {
	select {
	case nb := <-s.batches:
		return nb, nil
	case err := <-s.errs:
		return nil, err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// send sends a batch of creation notifications of the keys.
func (s *notificationsTestSubscription) send(offset int64, keys ...string) {
	nb := &proto.NotificationBatch{Shard: s.request.Shard, Offset: offset}
	for _, key := range keys {
		nb.Notifications = append(nb.Notifications, &proto.NotificationEntry{
			Key:   &key,
			Value: &proto.Notification{Type: proto.NotificationType_KEY_CREATED, VersionId: &offset},
		})
	}
	s.batches <- nb
}

func nextNotification(t *testing.T, nm *notifications) *Notification {
	t.Helper()
	select {
	case n := <-nm.Ch():
		require.NotNil(t, n)
		return n
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no notification")
		return nil
	}
}

func TestNotificationsFollowSplit(t *testing.T) {
	shardManager := &notificationsTestShardManager{shards: []int64{0}}
	server := newNotificationsTestServer()
	server.confirmAt(0, 10)
	nm, err := newNotifications(context.Background(), clientOptions{requestTimeout: 10 * time.Second},
		server, shardManager)
	require.NoError(t, err)
	defer func() { assert.NoError(t, nm.Close()) }()

	parent := server.nextSubscription(t)
	assert.Nil(t, parent.request.StartOffsetExclusive)
	parent.send(11, "a")
	assert.Equal(t, "a", nextNotification(t, nm).Key)

	// The parent keeps its stream open after the split: the subscription
	// cancels it, and subscribes to the children from the last batch it
	// received from the parent
	shardManager.split(0, 1, 2)
	assert.Eventually(t, func() bool { return parent.ctx.Err() != nil }, 10*time.Second, 10*time.Millisecond)

	children := map[int64]*notificationsTestSubscription{}
	for range 2 {
		child := server.nextSubscription(t)
		children[child.request.Shard] = child
		if assert.NotNil(t, child.request.StartOffsetExclusive, "shard %d", child.request.Shard) {
			assert.EqualValues(t, 11, *child.request.StartOffsetExclusive, "shard %d", child.request.Shard)
		}
	}
	require.Contains(t, children, int64(1))
	require.Contains(t, children, int64(2))

	children[1].send(12, "b")
	children[2].send(13, "c")
	keys := []string{nextNotification(t, nm).Key, nextNotification(t, nm).Key}
	assert.ElementsMatch(t, []string{"b", "c"}, keys)

	// A child split in turn hands over its subscription the same way
	shardManager.split(2, 3, 4)
	for range 2 {
		grandchild := server.nextSubscription(t)
		assert.Contains(t, []int64{3, 4}, grandchild.request.Shard)
		if assert.NotNil(t, grandchild.request.StartOffsetExclusive) {
			assert.EqualValues(t, 13, *grandchild.request.StartOffsetExclusive)
		}
	}
}

func TestNotificationsFollowSplitBeforeInitialized(t *testing.T) {
	shardManager := &notificationsTestShardManager{shards: []int64{0}}
	server := newNotificationsTestServer()
	server.reject(0, constant.ErrNodeIsNotLeader)
	server.leaveUnconfirmed(2)

	type result struct {
		nm  *notifications
		err error
	}
	created := make(chan result, 1)
	go func() {
		nm, err := newNotifications(context.Background(), clientOptions{requestTimeout: 30 * time.Second},
			server, shardManager)
		created <- result{nm, err}
	}()
	assert.Eventually(t, func() bool { return server.attemptsOn(0) > 0 }, 10*time.Second, 10*time.Millisecond)

	// The shard was split before the subscription was established on it: the
	// subscriptions to the children establish it in its place, without a
	// batch of the parent to start after
	shardManager.split(0, 1, 2)
	children := map[int64]*notificationsTestSubscription{}
	for range 2 {
		child := server.nextSubscription(t)
		children[child.request.Shard] = child
		assert.Nil(t, child.request.StartOffsetExclusive, "shard %d", child.request.Shard)
	}
	require.Contains(t, children, int64(1))
	require.Contains(t, children, int64(2))

	// Established once both children confirm their subscription
	select {
	case <-created:
		require.FailNow(t, "the subscription was established before the subscription to shard 2")
	case <-time.After(200 * time.Millisecond):
	}
	children[2].send(0)

	var res result
	select {
	case res = <-created:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the subscription was not established")
	}
	require.NoError(t, res.err)
	defer func() { assert.NoError(t, res.nm.Close()) }()

	children[1].send(1, "a")
	assert.Equal(t, "a", nextNotification(t, res.nm).Key)
}

func TestNotificationsMissed(t *testing.T) {
	shardManager := &notificationsTestShardManager{shards: []int64{0}}
	server := newNotificationsTestServer()
	server.confirmAt(0, 10)
	nm, err := newNotifications(context.Background(), clientOptions{requestTimeout: 10 * time.Second},
		server, shardManager)
	require.NoError(t, err)
	defer func() { assert.NoError(t, nm.Close()) }()

	subscription := server.nextSubscription(t)
	subscription.send(11, "a")
	assert.Equal(t, "a", nextNotification(t, nm).Key)

	// The subscription starts again after the last batch it received, but the
	// retention deleted the batches up to offset 20: the server confirms it
	// after them
	server.confirmAt(0, 20)
	subscription.errs <- constant.ErrResourceUnavailable
	subscription = server.nextSubscription(t)
	if assert.NotNil(t, subscription.request.StartOffsetExclusive) {
		assert.EqualValues(t, 11, *subscription.request.StartOffsetExclusive)
	}
	missed := nextNotification(t, nm)
	assert.Equal(t, NotificationsMissed, missed.Type)
	assert.Empty(t, missed.Key)
	assert.EqualValues(t, -1, missed.VersionId)

	subscription.send(21, "b")
	assert.Equal(t, "b", nextNotification(t, nm).Key)

	// Confirmed where it starts, a subscription missed nothing
	subscription.errs <- constant.ErrResourceUnavailable
	subscription = server.nextSubscription(t)
	if assert.NotNil(t, subscription.request.StartOffsetExclusive) {
		assert.EqualValues(t, 21, *subscription.request.StartOffsetExclusive)
	}
	subscription.send(22, "c")
	assert.Equal(t, "c", nextNotification(t, nm).Key)
}
