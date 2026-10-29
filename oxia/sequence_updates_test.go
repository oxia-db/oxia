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
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxia/internal"
)

type sequenceUpdatesTestShardManager struct {
	internal.ShardManager
}

func (*sequenceUpdatesTestShardManager) Get(string) int64 { return 0 }

func (*sequenceUpdatesTestShardManager) Leader(int64) string { return "server-1" }

// Returns the highest sequence keys in turn, then fails the next Recv() with
// err, once the delay has elapsed.
type sequenceUpdatesTestStream struct {
	grpc.ClientStream
	keys  []string
	delay time.Duration
	err   error
}

func (s *sequenceUpdatesTestStream) Recv() (*proto.GetSequenceUpdatesResponse, error) {
	if len(s.keys) > 0 {
		key := s.keys[0]
		s.keys = s.keys[1:]
		return &proto.GetSequenceUpdatesResponse{HighestSequenceKey: key}, nil
	}
	time.Sleep(s.delay)
	return nil, s.err
}

// Returns the stream, or fails the requests with err, if set.
type sequenceUpdatesTestRpcProvider struct {
	internal.RpcProvider
	stream   *sequenceUpdatesTestStream
	err      error
	requests atomic.Int64
}

func (p *sequenceUpdatesTestRpcProvider) GetSequenceUpdates(context.Context, string,
	*proto.GetSequenceUpdatesRequest) (proto.OxiaClient_GetSequenceUpdatesClient, error) {
	p.requests.Add(1)
	if p.err != nil {
		return nil, p.err
	}
	return p.stream, nil
}

type resetCountingBackOff struct {
	backoff.BackOff
	resets int
}

func (b *resetCountingBackOff) Reset() {
	b.resets++
	b.BackOff.Reset()
}

func newTestSequenceUpdates(stream *sequenceUpdatesTestStream) (*sequenceUpdates, *resetCountingBackOff) {
	bo := &resetCountingBackOff{BackOff: time2.NewBackOff(context.Background())}
	return &sequenceUpdates{
		prefixKey:    "a",
		partitionKey: "x",
		ch:           make(chan string),
		shardManager: &sequenceUpdatesTestShardManager{},
		rpcProvider:  &sequenceUpdatesTestRpcProvider{stream: stream},
		ctx:          context.Background(),
		backoff:      bo,
		log:          slog.Default(),
	}, bo
}

func TestSequenceUpdatesRejectionDoesNotResetBackoff(t *testing.T) {
	// The stream is created even when the server rejects the subscription: the
	// rejection is only reported by the first Recv()
	su, bo := newTestSequenceUpdates(&sequenceUpdatesTestStream{err: constant.ErrNodeIsNotLeader})

	assert.ErrorIs(t, su.getSequenceUpdates(), constant.ErrNodeIsNotLeader)
	assert.Zero(t, bo.resets)
}

func TestSequenceUpdatesAcceptedStreamResetsBackoff(t *testing.T) {
	// An accepted subscription gets no response until a sequence key exists.
	// When it fails (eg. the leader stepped down), it should be retried quickly.
	su, bo := newTestSequenceUpdates(&sequenceUpdatesTestStream{delay: sequenceUpdatesAcceptedAfter, err: io.EOF})

	assert.ErrorIs(t, su.getSequenceUpdates(), io.EOF)
	assert.Equal(t, 1, bo.resets)
}

func TestSequenceUpdatesGetSequenceUpdatesFailsOnRequestError(t *testing.T) {
	su, _ := newTestSequenceUpdates(nil)
	su.rpcProvider = &sequenceUpdatesTestRpcProvider{err: constant.ErrNodeIsNotLeader}

	assert.ErrorIs(t, su.getSequenceUpdates(), constant.ErrNodeIsNotLeader)
}

// A request that fails because the subscription was closed reports the close,
// not the error of the request.
func TestSequenceUpdatesGetSequenceUpdatesFailsWithContextErrorWhenClosed(t *testing.T) {
	su, _ := newTestSequenceUpdates(nil)
	su.rpcProvider = &sequenceUpdatesTestRpcProvider{err: constant.ErrNodeIsNotLeader}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	su.ctx = ctx

	assert.ErrorIs(t, su.getSequenceUpdates(), context.Canceled)
}

// The first response is empty when no sequence key exists yet: it is not an
// update.
func TestSequenceUpdatesGetSequenceUpdatesSkipsEmptyKey(t *testing.T) {
	su, _ := newTestSequenceUpdates(&sequenceUpdatesTestStream{keys: []string{"", "a-1", "a-2"}, err: io.EOF})

	done := make(chan error, 1)
	go func() { done <- su.getSequenceUpdates() }()

	assert.Equal(t, "a-1", <-su.ch)
	assert.Equal(t, "a-2", <-su.ch)
	assert.ErrorIs(t, <-done, io.EOF)
}

// An update that nobody receives does not keep the subscription from ending.
func TestSequenceUpdatesGetSequenceUpdatesEndsWhenContextIsDone(t *testing.T) {
	su, _ := newTestSequenceUpdates(&sequenceUpdatesTestStream{keys: []string{"a-1"}, err: io.EOF})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	su.ctx = ctx

	assert.ErrorIs(t, su.getSequenceUpdates(), context.Canceled)
}

// The failed subscriptions are retried, until the retries end, and the
// channel is closed then.
func TestSequenceUpdatesGetSequenceUpdatesWithRetries(t *testing.T) {
	su, _ := newTestSequenceUpdates(nil)
	provider := &sequenceUpdatesTestRpcProvider{err: constant.ErrNodeIsNotLeader}
	su.rpcProvider = provider
	su.backoff = backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 2)

	su.getSequenceUpdatesWithRetries()

	_, open := <-su.ch
	assert.False(t, open)
	assert.EqualValues(t, 3, provider.requests.Load())
}
