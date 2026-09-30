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
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/ristretto"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/process"
)

// Cache provides a view of the data stored in Oxia that is locally cached.
//
// The cached values are automatically updated when there are updates or
// deletions.
// The cache is storing de-serialized object.
type Cache[Value any] interface {
	io.Closer

	// Put Associates a value with a key
	//
	// There are few options that can be passed to the Put operation:
	//  - The Put operation can be made conditional on that the record hasn't changed from
	//    a specific existing version by passing the [ExpectedVersionId] option.
	//  - Client can assert that the record does not exist by passing [ExpectedRecordNotExists]
	//  - Client can create an ephemeral record with [Ephemeral]
	//
	// Returns a [Version] object that contains information about the newly updated record
	// Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
	// current version id of the record
	Put(ctx context.Context, key string, value Value, options ...PutOption) (string, Version, error)

	// ReadModifyUpdate applies atomically the result of the `modifyFunc` argument into
	// the database.
	// The `modifyFunc` will be pass the current value and will return the modified value
	ReadModifyUpdate(ctx context.Context, key string, modifyFunc ModifyFunc[Value]) error

	// Delete removes the key and its associated value from the data store.
	//
	// The Delete operation can be made conditional on that the record hasn't changed from
	// a specific existing version by passing the [ExpectedVersionId] option.
	// Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
	// current version id of the record
	Delete(ctx context.Context, key string, options ...DeleteOption) error

	// Get returns the value associated with the specified key.
	// In addition to the value, a version object is also returned, with information
	// about the record state.
	// Returns ErrorKeyNotFound if the record does not exist
	Get(ctx context.Context, key string) (Value, Version, error)
}

// ModifyFunc is the transformation function to apply on ReadModifyUpdate.
type ModifyFunc[Value any] func(v Optional[Value]) (Value, error)

// SerializeFunc is the serialization function. eg: [json.Marshall].
type SerializeFunc func(value any) ([]byte, error)

// DeserializeFunc is the deserialization function. eg: [json.Unmarshall].
type DeserializeFunc func(data []byte, value any) error

// NewCache creates a new cache object for a specific type
// Uses the `serializeFunc` and `deserializeFunc` for SerDe.
func NewCache[T any](client SyncClient, serializeFunc SerializeFunc, deserializeFunc DeserializeFunc) (Cache[T], error) {
	c, ok := client.(*syncClientImpl)
	if !ok {
		return nil, errors.New("Invalid client implementation")
	}

	cm, err := c.getCacheManager()
	if err != nil {
		return nil, err
	}

	return newCache[T](cm, serializeFunc, deserializeFunc)
}

const (
	defaultCacheTTL = 5 * time.Minute
)

type cacheManager struct {
	sync.Mutex

	client        SyncClient
	notifications Notifications
	caches        []internalCache

	ctx    context.Context
	cancel context.CancelFunc
}

func newCacheManager(client SyncClient) (*cacheManager, error) {
	cm := &cacheManager{
		client: client,
	}

	cm.ctx, cm.cancel = context.WithCancel(context.Background())

	var err error
	if cm.notifications, err = client.GetNotifications(); err != nil {
		return nil, errors.Wrap(err, "failed to create notifications client")
	}

	go process.DoWithLabels(
		cm.ctx,
		map[string]string{
			"oxia": "cache-manager",
		},
		cm.run,
	)

	return cm, nil
}

func (cm *cacheManager) run() {
	for {
		select {
		case n := <-cm.notifications.Ch():
			if n == nil {
				return
			}
			cm.handleNotification(n)

		case <-cm.ctx.Done():
			// Cache manager closed
			return
		}
	}
}

func (cm *cacheManager) handleNotification(n *Notification) {
	cm.Lock()
	defer cm.Unlock()

	slog.Debug(
		"Received notification",
		slog.String("key", n.Key),
		slog.Any("type", n.Type),
		slog.Int64("version-id", n.VersionId),
	)

	for _, c := range cm.caches {
		c.handleNotification(n)
	}
}

func newCache[T any](cm *cacheManager, serializeFunc SerializeFunc, deserializeFunc DeserializeFunc) (Cache[T], error) {
	cm.Lock()
	defer cm.Unlock()

	cache, err := newCacheImpl[T](cm.client, serializeFunc, deserializeFunc)
	if err != nil {
		return nil, err
	}
	cm.caches = append(cm.caches, cache)
	return cache, nil
}

func (cm *cacheManager) Close() error {
	cm.cancel()

	cm.Lock()
	defer cm.Unlock()

	err := cm.notifications.Close()

	for _, c := range cm.caches {
		err = multierr.Append(err, c.Close())
	}

	return err
}

type internalCache interface {
	io.Closer
	handleNotification(n *Notification)
}

type cacheImpl[Value any] struct {
	sync.RWMutex

	client          SyncClient
	serializeFunc   SerializeFunc
	deserializeFunc DeserializeFunc
	valueCache      *ristretto.Cache

	// The keys that Get is loading from the server, with their evictions: a key
	// evicted while it loads can have changed after the server read the record,
	// and the load must not cache that record then.
	loadsMutex sync.Mutex
	loads      map[string]*keyLoads
}

// The loads of a key in flight, and the evictions of the key since the first one
// started.
type keyLoads struct {
	count     int
	evictions uint64
}

func newCacheImpl[Value any](client SyncClient, serializeFunc SerializeFunc, deserializeFunc DeserializeFunc) (*cacheImpl[Value], error) {
	c := &cacheImpl[Value]{
		client:          client,
		serializeFunc:   serializeFunc,
		deserializeFunc: deserializeFunc,
		loads:           map[string]*keyLoads{},
	}

	var err error
	c.valueCache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1_000_000,
		MaxCost:     64 * 1024 * 1024,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *cacheImpl[Value]) handleNotification(n *Notification) {
	c.RLock()
	defer c.RUnlock()

	if n.Type == KeyRangeRangeDeleted {
		// Ristretto can't list the cached keys between the bounds of the range,
		// so drop all the cached records
		c.evictAll()
		return
	}
	c.evict(n.Key)
}

// The eviction and the Set of a load that raced it are ordered by the loads
// mutex: either the load sees the eviction and skips its Set, or ristretto
// applies the Del after the Set, as both go through its buffer in order.
func (c *cacheImpl[Value]) evict(key string) {
	c.loadsMutex.Lock()
	defer c.loadsMutex.Unlock()

	if kl, ok := c.loads[key]; ok {
		kl.evictions++
	}
	c.valueCache.Del(key)
}

func (c *cacheImpl[Value]) evictAll() {
	c.loadsMutex.Lock()
	defer c.loadsMutex.Unlock()

	for _, kl := range c.loads {
		kl.evictions++
	}
	c.valueCache.Clear()
}

func (c *cacheImpl[Value]) Put(ctx context.Context, key string, value Value, options ...PutOption) (string, Version, error) {
	data, err := c.serializeFunc(value)
	if err != nil {
		return "", Version{}, errors.Wrap(err, "failed to serialize value")
	}

	insertedKey, version, err := c.client.Put(ctx, key, data, options...)
	if !errors.Is(err, ErrUnexpectedVersionId) {
		c.evict(key)
	}

	return insertedKey, version, err
}

func (c *cacheImpl[Value]) Delete(ctx context.Context, key string, options ...DeleteOption) error {
	err := c.client.Delete(ctx, key, options...)
	c.evict(key)
	return err
}

func (c *cacheImpl[Value]) Get(ctx context.Context, key string) (value Value, version Version, err error) {
	if cachedValue, cached := c.valueCache.Get(key); cached {
		if cv, present := cachedValue.(cachedResult[Value]).Get(); present {
			return cv.value, cv.version, nil
		}

		return value, version, ErrKeyNotFound
	}

	return c.load(ctx, key)
}

func (c *cacheImpl[Value]) load(ctx context.Context, key string) (value Value, version Version, err error) {
	kl, evictions := c.startLoad(key)
	defer c.endLoad(key, kl)

	// Cache the record only if the key was not evicted since the load started,
	// as the eviction can be of a change applied after the server read it
	set := func(cr cachedResult[Value], cost int64, ttl time.Duration) {
		c.loadsMutex.Lock()
		defer c.loadsMutex.Unlock()

		if kl.evictions == evictions {
			c.valueCache.SetWithTTL(key, cr, cost, ttl)
		}
	}

	_, data, existingVersion, err := c.client.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		// Use valueVersion[Value] instead of cachedResult[Value] to avoid double-wrapping.
		cr := empty[valueVersion[Value]]()
		set(cr, 0, 0)
		return value, version, err
	}

	if err != nil {
		return value, version, err
	}

	if err := c.deserializeFunc(data, &value); err != nil {
		return value, version, errors.Wrap(err, "failed to deserialize value")
	}

	cr := optionalOf[valueVersion[Value]](valueVersion[Value]{
		value:   value,
		version: existingVersion,
	})

	set(cr, int64(len(data)), defaultCacheTTL)
	return value, existingVersion, nil
}

// Registers a load of the key, and returns the evictions of the key so far.
func (c *cacheImpl[Value]) startLoad(key string) (*keyLoads, uint64) {
	c.loadsMutex.Lock()
	defer c.loadsMutex.Unlock()

	kl, ok := c.loads[key]
	if !ok {
		kl = &keyLoads{}
		c.loads[key] = kl
	}
	kl.count++
	return kl, kl.evictions
}

func (c *cacheImpl[Value]) endLoad(key string, kl *keyLoads) {
	c.loadsMutex.Lock()
	defer c.loadsMutex.Unlock()

	kl.count--
	if kl.count == 0 {
		delete(c.loads, key)
	}
}

func (c *cacheImpl[Value]) ReadModifyUpdate(ctx context.Context, key string, modifyFunc ModifyFunc[Value]) error {
	return backoff.Retry(func() error {
		var optValue Optional[Value]
		var versionId int64
		existingValue, version, err := c.Get(ctx, key)

		switch {
		case errors.Is(err, ErrKeyNotFound):
			optValue = empty[Value]()
			versionId = VersionIdNotExists
		case err != nil:
			return backoff.Permanent(err)
		default:
			optValue = optionalOf(existingValue)
			versionId = version.VersionId
		}

		newValue, err := modifyFunc(optValue)
		if err != nil {
			return backoff.Permanent(err)
		}

		_, _, err = c.Put(ctx, key, newValue, ExpectedVersionId(versionId))
		if err != nil {
			if errors.Is(err, ErrUnexpectedVersionId) {
				// Retry on conflict
				return err
			}

			return backoff.Permanent(err)
		}

		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
}

func (c *cacheImpl[Value]) Close() error {
	c.Lock()
	defer c.Unlock()

	c.valueCache.Close()
	return nil
}

type cachedResult[Value any] Optional[valueVersion[Value]]

type valueVersion[Value any] struct {
	value   Value
	version Version
}
