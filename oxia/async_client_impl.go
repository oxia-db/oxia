// Copyright 2023 StreamNative, Inc.
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
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"oxia/common"
	"oxia/oxia/internal"
	"oxia/oxia/internal/batch"
	"oxia/oxia/internal/metrics"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"sync"
)

type clientImpl struct {
	sync.Mutex
	options           clientOptions
	shardManager      internal.ShardManager
	writeBatchManager *batch.Manager
	readBatchManager  *batch.Manager
	sessions          *sessions

	clientPool common.ClientPool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewAsyncClient creates a new Oxia client with the async interface
//
// ServiceAddress is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
// shard assignments. Ideally this should be a load-balanced endpoint.
//
// A list of ClientOption arguments can be passed to configure the Oxia client
func NewAsyncClient(serviceAddress string, opts ...ClientOption) (AsyncClient, error) {
	clientPool := common.NewClientPool()

	options, err := newClientOptions(serviceAddress, opts...)
	if err != nil {
		return nil, err
	}

	shardManager, err := internal.NewShardManager(internal.NewShardStrategy(), clientPool, serviceAddress, options.requestTimeout)
	if err != nil {
		return nil, err
	}

	executor := &internal.ExecutorImpl{
		ClientPool:     clientPool,
		ShardManager:   shardManager,
		ServiceAddress: options.serviceAddress,
	}
	batcherFactory := batch.NewBatcherFactory(
		executor,
		options.batchLinger,
		options.maxRequestsPerBatch,
		metrics.NewMetrics(options.meterProvider),
		options.requestTimeout)
	c := &clientImpl{
		options:           options,
		clientPool:        clientPool,
		shardManager:      shardManager,
		writeBatchManager: batch.NewManager(batcherFactory.NewWriteBatcher),
		readBatchManager:  batch.NewManager(batcherFactory.NewReadBatcher),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.sessions = newSessions(c.ctx, c.shardManager, c.clientPool, c.options)
	return c, nil
}

func (c *clientImpl) Close() error {
	c.cancel()

	return multierr.Combine(
		c.writeBatchManager.Close(),
		c.readBatchManager.Close(),
		c.clientPool.Close(),
	)
}

func (c *clientImpl) Put(key string, payload []byte, options ...PutOption) <-chan PutResult {
	ch := make(chan PutResult, 1)
	shardId := c.shardManager.Get(key)
	callback := func(response *proto.PutResponse, err error) {
		if err != nil {
			ch <- PutResult{Err: err}
		} else {
			ch <- toPutResult(response)
		}
		close(ch)
	}
	opts := newPutOptions(options)
	putCall := model.PutCall{
		Key:               key,
		Payload:           payload,
		ExpectedVersionId: opts.expectedVersion,
		Callback:          callback,
	}
	if opts.ephemeral {
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
	shardId := c.shardManager.Get(key)
	callback := func(response *proto.DeleteResponse, err error) {
		if err != nil {
			ch <- err
		} else {
			ch <- toDeleteResult(response)
		}
		close(ch)
	}
	opts := newDeleteOptions(options)
	c.writeBatchManager.Get(shardId).Add(model.DeleteCall{
		Key:               key,
		ExpectedVersionId: opts.expectedVersion,
		Callback:          callback,
	})
	return ch
}

func (c *clientImpl) DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error {
	shardIds := c.shardManager.GetAll()
	ch := make(chan error, 1)
	var eg errgroup.Group
	for _, shardId := range shardIds {
		chInner := make(chan error, 1)
		callback := func(response *proto.DeleteRangeResponse, err error) {
			if err != nil {
				chInner <- err
			} else {
				chInner <- toDeleteRangeResult(response)
			}
		}
		c.writeBatchManager.Get(shardId).Add(model.DeleteRangeCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			Callback:        callback,
		})
		eg.Go(func() error {
			return <-chInner
		})
	}
	go func() {
		ch <- eg.Wait()
		close(ch)
	}()
	return ch
}

func (c *clientImpl) Get(key string) <-chan GetResult {
	ch := make(chan GetResult, 1)
	shardId := c.shardManager.Get(key)
	callback := func(response *proto.GetResponse, err error) {
		if err != nil {
			ch <- GetResult{Err: err}
		} else {
			ch <- toGetResult(response)
		}
		close(ch)
	}
	c.readBatchManager.Get(shardId).Add(model.GetCall{
		Key:      key,
		Callback: callback,
	})
	return ch
}

func (c *clientImpl) List(minKeyInclusive string, maxKeyExclusive string) <-chan ListResult {
	shardIds := c.shardManager.GetAll()
	ch := make(chan ListResult, 1)
	var wg sync.WaitGroup
	wg.Add(len(shardIds))
	keys := make([]string, 0)
	for _, shardId := range shardIds {
		cInner := make(chan ListResult, 1)
		callback := func(response *proto.ListResponse, err error) {
			if err != nil {
				cInner <- ListResult{Err: err}
			} else {
				cInner <- toListResult(response)
			}
		}
		c.readBatchManager.Get(shardId).Add(model.ListCall{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
			Callback:        callback,
		})
		go func() {
			x := <-cInner
			keys = append(keys, x.Keys...)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		ch <- ListResult{
			Keys: keys,
		}
		close(ch)
	}()
	return ch
}

func (c *clientImpl) GetNotifications() (Notifications, error) {
	nm, err := newNotifications(c.options, c.ctx, c.clientPool, c.shardManager)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create notification stream")
	}

	return nm, nil
}
