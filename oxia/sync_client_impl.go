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
	"go.uber.org/multierr"
	"sync"
)

type syncClientImpl struct {
	sync.Mutex

	asyncClient  AsyncClient
	cacheManager *cacheManager
}

// NewSyncClient creates a new Oxia client with the sync interface
//
// ServiceAddress is the target host:port of any Oxia server to bootstrap the client. It is used for establishing the
// shard assignments. Ideally this should be a load-balanced endpoint.
//
// A list of ClientOption arguments can be passed to configure the Oxia client.
// Example:
//
//	client, err := oxia.NewSyncClient("my-oxia-service:6648", oxia.WithRequestTimeout(30*time.Second))
func NewSyncClient(serviceAddress string, opts ...ClientOption) (SyncClient, error) {
	options := append(opts, WithBatchLinger(0))

	asyncClient, err := NewAsyncClient(serviceAddress, options...)
	if err != nil {
		return nil, err
	}
	return newSyncClient(asyncClient), nil
}

func newSyncClient(asyncClient AsyncClient) SyncClient {
	return &syncClientImpl{
		asyncClient:  asyncClient,
		cacheManager: nil,
	}
}

func (c *syncClientImpl) getCacheManager() (*cacheManager, error) {
	c.Lock()
	defer c.Unlock()

	if c.cacheManager != nil {
		return c.cacheManager, nil
	}

	var err error
	c.cacheManager, err = newCacheManager(c)
	return c.cacheManager, err
}

func (c *syncClientImpl) Close() error {
	c.Lock()
	defer c.Unlock()

	var err error
	if c.cacheManager != nil {
		err = c.cacheManager.Close()
	}
	return multierr.Combine(err, c.asyncClient.Close())
}

func (c *syncClientImpl) Put(ctx context.Context, key string, value []byte, options ...PutOption) (Version, error) {
	select {
	case r := <-c.asyncClient.Put(key, value, options...):
		return r.Version, r.Err
	case <-ctx.Done():
		return Version{}, ctx.Err()
	}
}

func (c *syncClientImpl) Delete(ctx context.Context, key string, options ...DeleteOption) error {
	select {
	case err := <-c.asyncClient.Delete(key, options...):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *syncClientImpl) DeleteRange(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) error {
	select {
	case err := <-c.asyncClient.DeleteRange(minKeyInclusive, maxKeyExclusive):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *syncClientImpl) Get(ctx context.Context, key string) ([]byte, Version, error) {
	select {
	case r := <-c.asyncClient.Get(key):
		return r.Value, r.Version, r.Err
	case <-ctx.Done():
		return nil, Version{}, ctx.Err()
	}
}

func (c *syncClientImpl) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) <-chan ListResult {
	return c.asyncClient.List(ctx, minKeyInclusive, maxKeyExclusive)
}

func (c *syncClientImpl) GetNotifications() (Notifications, error) {
	return c.asyncClient.GetNotifications()
}
