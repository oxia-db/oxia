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
	shards []int64
}

func (m *notificationsTestShardManager) GetAll() []int64 { return m.shards }

func (*notificationsTestShardManager) Leader(int64) string { return "server-1" }

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

// Fails the first Recv() with err, once the delay has elapsed.
type notificationsTestFailingStream struct {
	grpc.ClientStream
	delay time.Duration
	err   error
}

func (s *notificationsTestFailingStream) Recv() (*proto.NotificationBatch, error) {
	time.Sleep(s.delay)
	return nil, s.err
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
	// rejection is only reported by the first Recv()
	snm, bo := newTestShardNotificationsManager(&notificationsTestFailingStream{err: constant.ErrNodeIsNotLeader})

	assert.ErrorIs(t, snm.getNotifications(), constant.ErrNodeIsNotLeader)
	assert.Zero(t, bo.resets)
}

func TestNotificationsAcceptedStreamResetsBackoff(t *testing.T) {
	// A subscription that resumes from an offset gets no response until a new
	// notification is written. When it fails (eg. the leader stepped down), it
	// should be retried quickly.
	snm, bo := newTestShardNotificationsManager(&notificationsTestFailingStream{
		delay: notificationsAcceptedAfter,
		err:   constant.ErrResourceUnavailable,
	})
	snm.lastOffsetReceived = 5

	assert.ErrorIs(t, snm.getNotifications(), constant.ErrResourceUnavailable)
	assert.Equal(t, 1, bo.resets)
}
