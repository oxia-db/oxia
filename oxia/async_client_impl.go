// Copyright 2023-2025 The Oxia Authors
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
	"container/heap"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
	time2 "github.com/oxia-db/oxia/common/time"
	commonbatch "github.com/oxia-db/oxia/oxia/batch"

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal"
	"github.com/oxia-db/oxia/oxia/internal/batch"
	"github.com/oxia-db/oxia/oxia/internal/metrics"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

type clientImpl struct {
	sync.Mutex
	options           clientOptions
	shardManager      internal.ShardManager
	writeBatchManager *batch.Manager
	readBatchManager  *batch.Manager
	executor          internal.Executor
	sessions          *sessions
	notifications     []*notifications

	clientPool rpc.ClientPool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewAsyncClient creates a new Oxia client with the async interface
//
// ServiceAddress is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
// shard assignments. Ideally this should be a load-balanced endpoint.
//
// A list of ClientOption arguments can be passed to configure the Oxia client.
// Example:
//
//	client, err := oxia.NewAsyncClient("my-oxia-service:6648", oxia.WithBatchLinger(10*time.Milliseconds))
func NewAsyncClient(serviceAddress string, opts ...ClientOption) (AsyncClient, error) {
	var err error
	options, err := newClientOptions(serviceAddress, opts...)
	if err != nil {
		return nil, err
	}

	clientPool := rpc.NewClientPool(options.tls, options.authentication)

	var shardManager internal.ShardManager
	if options.failureInjection.Contains(DizzyShardManager) {
		shardManager, err = internal.NewDizzyShardManager(internal.NewShardStrategy(), clientPool, serviceAddress,
			options.namespace, options.requestTimeout)
	} else {
		shardManager, err = internal.NewShardManager(internal.NewShardStrategy(), clientPool, serviceAddress,
			options.namespace, options.requestTimeout)
	}
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	executor := internal.NewExecutor(ctx, options.namespace, clientPool, shardManager, options.serviceAddress)
	batcherFactory := batch.NewBatcherFactory(
		executor,
		options.namespace,
		options.batchLinger,
		options.maxRequestsPerBatch,
		metrics.NewMetrics(options.meterProvider),
		options.requestTimeout)
	batcherFactory.ShardExists = shardManager.Exists
	c := &clientImpl{
		options:      options,
		clientPool:   clientPool,
		shardManager: shardManager,
		writeBatchManager: batch.NewManager(ctx, func(ctx context.Context, shard *int64) commonbatch.Batcher {
			return batcherFactory.NewWriteBatcher(ctx, shard, options.maxBatchSize)
		}),
		readBatchManager: batch.NewManager(ctx, batcherFactory.NewReadBatcher),
		executor:         executor,
	}

	c.ctx, c.cancel = ctx, cancel
	c.sessions = newSessions(c.ctx, c.shardManager, c.clientPool, c.options)

	// Set up re-routing callbacks for shard splits. When a batch detects its
	// target shard was deleted, these callbacks re-submit each operation through
	// the normal path (which re-hashes keys and routes to the correct child shards).
	batcherFactory.WriteRerouter = c.rerouteWrites
	batcherFactory.ReadRerouter = c.rerouteReads

	return c, nil
}

// rerouteWrites re-submits write operations to the correct child shards after
// the original target shard was deleted (e.g. shard split).
func (c *clientImpl) rerouteWrites(puts []model.PutCall, deletes []model.DeleteCall, deleteRanges []model.DeleteRangeCall) {
	for _, put := range puts {
		shardId := c.shardManager.Get(put.PartitionKeyOrKey())
		c.writeBatchManager.Get(shardId).Add(put)
	}
	for _, del := range deletes {
		shardId := c.shardManager.Get(del.Key)
		c.writeBatchManager.Get(shardId).Add(del)
	}
	for _, dr := range deleteRanges {
		// DeleteRanges without partition key are sent to all shards.
		// Re-submit to all current shards.
		for _, shardId := range c.shardManager.GetAll() {
			c.writeBatchManager.Get(shardId).Add(dr)
		}
	}
}

// rerouteReads re-submits read operations to the correct child shards.
func (c *clientImpl) rerouteReads(gets []model.GetCall) {
	for _, get := range gets {
		shardId := c.shardManager.Get(get.Key)
		c.readBatchManager.Get(shardId).Add(get)
	}
}

func (c *clientImpl) Close() error {
	err := multierr.Combine(
		c.sessions.Close(),
		c.writeBatchManager.Close(),
		c.readBatchManager.Close(),
		c.clientPool.Close(),
	)
	c.cancel()

	err = multierr.Append(err, c.closeNotifications())
	return err
}

func (c *clientImpl) Put(key string, value []byte, options ...PutOption) <-chan PutResult {
	ch := make(chan PutResult, 1)

	callback := func(response *proto.PutResponse, err error) {
		if err != nil {
			ch <- PutResult{Err: err}
		} else {
			ch <- toPutResult(key, response)
		}
		close(ch)
	}

	opts, err := newPutOptions(options)
	if err != nil {
		callback(nil, err)
		return ch
	}

	shardId := c.getShardForKey(key, opts)
	putCall := model.PutCall{
		Key:                key,
		Value:              value,
		ExpectedVersionId:  opts.expectedVersion,
		SequenceKeysDeltas: opts.sequenceKeysDeltas,
		PartitionKey:       opts.partitionKey,
		Callback:           callback,
		SecondaryIndexes:   toSecondaryIndexes(opts.secondaryIndexes),
	}
	if opts.ephemeral {
		putCall.ClientIdentity = &c.options.identity
		c.sessions.executeWithSessionId(shardId, func(sessionId int64, err error) {
			if err != nil {
				callback(nil, err)
				return
			}
			putCall.SessionId = &sessionId
			c.writeBatchManager.Get(shardId).Add(putCall)
		})
	} else {
		c.writeBatchManager.Get(shardId).Add(putCall)
	}
	return ch
}

func (c *clientImpl) Delete(key string, options ...DeleteOption) <-chan error {
	ch := make(chan error, 1)
	callback := func(response *proto.DeleteResponse, err error) {
		if err != nil {
			ch <- err
		} else {
			ch <- toDeleteResult(response)
		}
		close(ch)
	}
	opts := newDeleteOptions(options)
	shardId := c.getShardForKey(key, opts)
	c.writeBatchManager.Get(shardId).Add(model.DeleteCall{
		Key:               key,
		ExpectedVersionId: opts.expectedVersion,
		Callback:          callback,
	})
	return ch
}

func (c *clientImpl) DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) <-chan error {
	ch := make(chan error, 1)
	opts := newDeleteRangeOptions(options)
	if opts.partitionKey != nil {
		shardId := c.getShardForKey("", opts)
		c.doSingleShardDeleteRange(shardId, minKeyInclusive, maxKeyExclusive, ch)
		return ch
	}

	// If there is no partition key, we will make the request to delete-range on all the shards
	shardIDs := c.shardManager.GetAll()
	wg := concurrent.NewWaitGroup(len(shardIDs))

	for _, shardId := range shardIDs {
		// chInner := make(chan error, 1)
		c.writeBatchManager.Get(shardId).Add(model.DeleteRangeCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			Callback: func(response *proto.DeleteRangeResponse, err error) {
				if err != nil {
					wg.Fail(err)
					return
				}

				switch response.Status {
				case proto.Status_OK:
					wg.Done()
				default:
					wg.Fail(toError(response.Status))
				}
			},
		})
	}
	go func() {
		ch <- wg.Wait(c.ctx)
		close(ch)
	}()
	return ch
}

func (c *clientImpl) doSingleShardDeleteRange(shardId int64, minKeyInclusive string, maxKeyExclusive string, ch chan error) {
	c.writeBatchManager.Get(shardId).Add(model.DeleteRangeCall{
		MinKeyInclusive: minKeyInclusive,
		MaxKeyExclusive: maxKeyExclusive,
		Callback: func(response *proto.DeleteRangeResponse, err error) {
			if err != nil {
				ch <- err
			} else {
				ch <- toDeleteRangeResult(response)
			}

			close(ch)
		},
	})
}

func (c *clientImpl) Get(key string, options ...GetOption) <-chan GetResult {
	ch := make(chan GetResult)

	opts := newGetOptions(options)
	if opts.partitionKey == nil && //
		(opts.comparisonType != proto.KeyComparisonType_EQUAL ||
			opts.secondaryIndexName != nil) {
		c.doMultiShardGet(key, opts, ch)
	} else {
		c.doSingleShardGet(key, opts, ch)
	}

	return ch
}

func (c *clientImpl) doSingleShardGet(key string, opts *getOptions, ch chan GetResult) {
	shardId := c.getShardForKey(key, opts)
	c.readBatchManager.Get(shardId).Add(model.GetCall{
		Key:                key,
		ComparisonType:     opts.comparisonType,
		IncludeValue:       opts.includeValue,
		SecondaryIndexName: opts.secondaryIndexName,
		Callback: func(response *proto.GetResponse, err error) {
			ch <- toGetResult(response, key, err)
			close(ch)
		},
	})
}

func compareGetResponse(a, b *proto.GetResponse) int {
	if a.SecondaryIndexKey != nil && b.SecondaryIndexKey != nil {
		c := compare.CompareWithSlash([]byte(a.GetSecondaryIndexKey()), []byte(b.GetSecondaryIndexKey()))
		if c != 0 {
			return c
		}
	}
	return compare.CompareWithSlash([]byte(a.GetKey()), []byte(b.GetKey()))
}

func validateComparisonType(c proto.KeyComparisonType) error {
	switch c {
	case proto.KeyComparisonType_EQUAL,
		proto.KeyComparisonType_FLOOR,
		proto.KeyComparisonType_LOWER,
		proto.KeyComparisonType_CEILING,
		proto.KeyComparisonType_HIGHER:
		return nil
	default:
		return fmt.Errorf("unsupported comparison type: %v", c)
	}
}

var keyNotFound = &proto.GetResponse{
	Status: proto.Status_KEY_NOT_FOUND,
}

func selectResponse(kc proto.KeyComparisonType, selected *proto.GetResponse, response *proto.GetResponse) *proto.GetResponse {
	if response != nil && response.Status == proto.Status_OK {
		switch kc {
		case proto.KeyComparisonType_EQUAL:
			if selected == keyNotFound {
				selected = response
			}

		case proto.KeyComparisonType_FLOOR, proto.KeyComparisonType_LOWER:
			if selected == keyNotFound || compareGetResponse(selected, response) < 0 {
				selected = response
			}

		case proto.KeyComparisonType_CEILING, proto.KeyComparisonType_HIGHER:
			if selected == keyNotFound || compareGetResponse(selected, response) > 0 {
				selected = response
			}
		default:
			panic(fmt.Sprintf("unknown comparison type: %v", kc))
		}
	}
	return selected
}

// The keys might get hashed to multiple shards, so we have to check on all shards and then compare the results.
func (c *clientImpl) doMultiShardGet(key string, options *getOptions, ch chan GetResult) {
	if err := validateComparisonType(options.comparisonType); err != nil {
		ch <- toGetResult(nil, key, err)
		close(ch)
		return
	}

	m := sync.Mutex{}
	shards := c.shardManager.GetAll()
	counter := len(shards)
	selected := keyNotFound

	for _, shardId := range shards {
		c.readBatchManager.Get(shardId).Add(model.GetCall{
			Key:                key,
			ComparisonType:     options.comparisonType,
			IncludeValue:       options.includeValue,
			SecondaryIndexName: options.secondaryIndexName,
			Callback: func(response *proto.GetResponse, err error) {
				m.Lock()
				defer m.Unlock()

				if counter == 0 {
					// Response already sent, nothing to do
					return
				}

				if err != nil {
					ch <- toGetResult(nil, key, err)
					close(ch)
					counter = 0
				}

				selected = selectResponse(options.comparisonType, selected, response)

				counter--
				if counter == 0 {
					ch <- toGetResult(selected, key, nil)
					close(ch)
				}
			},
		})
	}
}

func (c *clientImpl) listFromShard(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, showInternalKeys bool, shardId int64, secondaryIndexName *string,
	ch chan<- ListResult) {
	request := &proto.ListRequest{
		Shard:               &shardId,
		StartInclusive:      minKeyInclusive,
		EndExclusive:        maxKeyExclusive,
		SecondaryIndexName:  secondaryIndexName,
		IncludeInternalKeys: showInternalKeys,
	}

	retryCtx, cancel := context.WithTimeout(ctx, c.options.requestTimeout)
	defer cancel()

	backOff := time2.NewBackOff(retryCtx)
	var hint *proto.LeaderHint

	err := backoff.RetryNotify(func() error {
		return c.doList(retryCtx, request, hint, ch)
	}, backOff, func(err error, duration time.Duration) {
		slog.Warn(
			"Failed to perform list request, retrying later",
			slog.Any("error", err),
			slog.String("namespace", c.options.namespace),
			slog.Int64("shard", shardId),
			slog.Duration("retry-after", duration),
		)
		if leaderHint := constant.FindLeaderHint(err); leaderHint != nil {
			hint = leaderHint
		}
	})
	if err != nil {
		ch <- ListResult{Err: err}
	}
}

func (c *clientImpl) doList(ctx context.Context, request *proto.ListRequest, hint *proto.LeaderHint, ch chan<- ListResult) error {
	client, err := c.executor.ExecuteList(ctx, request, hint)
	if err != nil {
		if batch.IsRetriable(err) {
			return err
		}
		return backoff.Permanent(err)
	}

	dataSent := false
	for {
		response, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Only retry if no data has been sent to the channel yet,
			// to avoid sending duplicate keys.
			if !dataSent && batch.IsRetriable(err) {
				return err
			}
			return backoff.Permanent(err)
		}

		ch <- ListResult{Keys: response.Keys}
		dataSent = true
	}
}

func (c *clientImpl) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...ListOption) <-chan ListResult {
	ch := make(chan ListResult)

	opts := newListOptions(options)
	if opts.partitionKey != nil {
		// If the partition key is specified, we only need to make the request to one shard
		shardId := c.getShardForKey("", opts)
		go func() {
			c.listFromShard(ctx, minKeyInclusive, maxKeyExclusive, opts.showInternalKeys, shardId, opts.secondaryIndexName, ch)
			close(ch)
		}()
	} else {
		// Do the list on all shards and aggregate the responses
		shardIDs := c.shardManager.GetAll()

		wg := concurrent.NewWaitGroup(len(shardIDs))
		for _, shardId := range shardIDs {
			shardIdPtr := shardId
			go func() {
				defer wg.Done()

				c.listFromShard(ctx, minKeyInclusive, maxKeyExclusive, opts.showInternalKeys, shardIdPtr, opts.secondaryIndexName, ch)
			}()
		}

		go func() {
			_ = wg.Wait(ctx)
			close(ch)
		}()
	}

	return ch
}

func (c *clientImpl) rangeScanFromShard(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, includeInternalKeys bool, shardId int64, secondaryIndexName *string,
	ch chan<- GetResult) {
	request := &proto.RangeScanRequest{
		Shard:               &shardId,
		StartInclusive:      minKeyInclusive,
		EndExclusive:        maxKeyExclusive,
		SecondaryIndexName:  secondaryIndexName,
		IncludeInternalKeys: includeInternalKeys,
	}

	retryCtx, cancel := context.WithTimeout(ctx, c.options.requestTimeout)
	defer cancel()

	backOff := time2.NewBackOff(retryCtx)
	var hint *proto.LeaderHint

	err := backoff.RetryNotify(func() error {
		return c.doRangeScan(retryCtx, request, hint, ch)
	}, backOff, func(err error, duration time.Duration) {
		slog.Warn(
			"Failed to perform range-scan request, retrying later",
			slog.Any("error", err),
			slog.String("namespace", c.options.namespace),
			slog.Int64("shard", shardId),
			slog.Duration("retry-after", duration),
		)
		if leaderHint := constant.FindLeaderHint(err); leaderHint != nil {
			hint = leaderHint
		}
	})
	if err != nil {
		ch <- GetResult{Err: err}
	}

	close(ch)
}

func (c *clientImpl) doRangeScan(ctx context.Context, request *proto.RangeScanRequest, hint *proto.LeaderHint, ch chan<- GetResult) error {
	client, err := c.executor.ExecuteRangeScan(ctx, request, hint)
	if err != nil {
		if batch.IsRetriable(err) {
			return err
		}
		return backoff.Permanent(err)
	}

	dataSent := false
	for {
		response, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Only retry if no data has been sent to the channel yet,
			// to avoid sending duplicate records.
			if !dataSent && batch.IsRetriable(err) {
				return err
			}
			return backoff.Permanent(err)
		}

		for _, record := range response.Records {
			ch <- toGetResult(record, "", nil)
		}
		dataSent = true
	}
}

func (c *clientImpl) RangeScan(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...RangeScanOption) <-chan GetResult {
	outCh := make(chan GetResult, 100)

	opts := newRangeScanOptions(options)
	if opts.partitionKey != nil {
		// If the partition key is specified, we only need to make the request to one shard
		shardId := c.getShardForKey("", opts)
		go func() {
			c.rangeScanFromShard(ctx, minKeyInclusive, maxKeyExclusive, opts.showInternalKeys, shardId, opts.secondaryIndexName, outCh)
		}()
	} else {
		// Do the list on all shards and aggregate the responses
		shardIDs := c.shardManager.GetAll()
		channels := make([]chan GetResult, len(shardIDs))

		for i, shardId := range shardIDs {
			shardIdPtr := shardId
			ch := make(chan GetResult)
			channels[i] = ch
			go func() {
				c.rangeScanFromShard(ctx, minKeyInclusive, maxKeyExclusive, opts.showInternalKeys, shardIdPtr, opts.secondaryIndexName, ch)
			}()
		}

		go aggregateAndSortRangeScanAcrossShards(channels, outCh)
	}

	return outCh
}

func (c *clientImpl) GetSequenceUpdates(ctx context.Context, prefixKey string, options ...GetSequenceUpdatesOption) (<-chan string, error) {
	opts := newGetSequenceUpdatesOptions(options)
	if opts.partitionKey == nil {
		return nil, errors.Wrap(ErrInvalidOptions, "partitionKey is required")
	}

	return newSequenceUpdates(ctx, prefixKey, *opts.partitionKey, c.clientPool, c.shardManager), nil
}

// We do range scan on all the shards, and we need to always pick the lowest key
// across all the shards.
func aggregateAndSortRangeScanAcrossShards(channels []chan GetResult, outCh chan GetResult) {
	h := &ResultHeap{}
	heap.Init(h)

	// First make sure we have 1 key from each channel
	for _, ch := range channels {
		if gr, ok := <-ch; ok {
			heap.Push(h, &ResultAndChannel{gr, ch})
		}
	}

	// Now that we have something from each channel, iterate by picking the
	// result with the lowest key and then reading again from that same
	// channel
	for h.Len() > 0 {
		r, ok := heap.Pop(h).(*ResultAndChannel)
		if !ok {
			panic("failed to cast")
		}

		outCh <- r.gr

		if r.gr.Err != nil {
			close(outCh)
			return
		}

		// read again from same channel
		if gr, ok := <-r.ch; ok {
			heap.Push(h, &ResultAndChannel{gr, r.ch})
		}
	}

	close(outCh)
}

func (c *clientImpl) closeNotifications() error {
	c.Lock()
	defer c.Unlock()

	var err error
	for _, n := range c.notifications {
		err = multierr.Append(err, n.Close())
	}

	return err
}

func (c *clientImpl) getShardForKey(key string, options baseOptionsIf) int64 {
	if options.PartitionKey() != nil {
		return c.shardManager.Get(*options.PartitionKey())
	}

	return c.shardManager.Get(key)
}

func (c *clientImpl) GetNotifications() (Notifications, error) {
	nm, err := newNotifications(c.ctx, c.options, c.clientPool, c.shardManager)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create notification stream")
	}

	c.Lock()
	defer c.Unlock()
	c.notifications = append(c.notifications, nm)

	return nm, nil
}
