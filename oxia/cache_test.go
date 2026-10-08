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
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Serves the records from memory.
type recordsClient struct {
	SyncClient

	sync.Mutex
	records map[string][]byte
	gets    atomic.Int64
	// Runs once, in the next Get, after it read the record
	afterNextRead func()
}

func (r *recordsClient) Put(_ context.Context, key string, value []byte, _ ...PutOption) (string, Version, error) {
	r.Lock()
	defer r.Unlock()

	r.records[key] = value
	return key, Version{}, nil
}

func (r *recordsClient) Delete(_ context.Context, key string, _ ...DeleteOption) error {
	r.Lock()
	defer r.Unlock()

	delete(r.records, key)
	return nil
}

func (r *recordsClient) Get(_ context.Context, key string, _ ...GetOption) (string, []byte, Version, error) {
	r.gets.Add(1)

	r.Lock()
	value, found := r.records[key]
	afterRead := r.afterNextRead
	r.afterNextRead = nil
	r.Unlock()

	if afterRead != nil {
		afterRead()
	}
	if !found {
		return "", nil, Version{}, ErrKeyNotFound
	}
	return key, value, Version{}, nil
}

// Changes the record as another client would.
func (r *recordsClient) write(key string, value *string) {
	if value == nil {
		_ = r.Delete(context.Background(), key)
		return
	}
	data, _ := json.Marshal(*value)
	_, _, _ = r.Put(context.Background(), key, data)
}

// Runs change while a Get of the cache is loading "/key", after the server read
// the record, and returns once the load is done.
func loadRacing(t *testing.T, client *recordsClient, change func(c *cacheImpl[string])) *cacheImpl[string] {
	t.Helper()

	c, err := newCacheImpl[string](client, json.Marshal, json.Unmarshal)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, c.Close())
	})

	read := make(chan struct{})
	release := make(chan struct{})
	client.afterNextRead = func() {
		close(read)
		<-release
	}

	loaded := make(chan struct{})
	go func() {
		defer close(loaded)
		_, _, _ = c.Get(context.Background(), "/key")
	}()

	<-read
	change(c)
	close(release)
	<-loaded

	// Apply the Set that the load buffered in ristretto, if any
	c.valueCache.Wait()
	return c
}

func assertRecord(t *testing.T, c *cacheImpl[string], expected *string) {
	t.Helper()

	value, _, err := c.Get(context.Background(), "/key")
	if expected == nil {
		assert.ErrorIs(t, err, ErrKeyNotFound, "value: %q", value)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, *expected, value)
	}

	c.loadsMutex.Lock()
	defer c.loadsMutex.Unlock()
	assert.Empty(t, c.loads)
}

// A notification that evicts a key while a Get is loading it from the server
// must not let the load cache the record as it was before the change.
func TestCache_LoadRacingNotification(t *testing.T) {
	v1, v2 := "v1", "v2"
	for _, test := range []struct {
		name         string
		before       *string
		after        *string
		notification Notification
	}{
		{"created", nil, &v1, Notification{Type: KeyCreated, Key: "/key"}},
		{"modified", &v1, &v2, Notification{Type: KeyModified, Key: "/key"}},
		{"deleted", &v1, nil, Notification{Type: KeyDeleted, Key: "/key"}},
		{"range-deleted", &v1, nil, Notification{Type: KeyRangeRangeDeleted, Key: "/a", KeyRangeEnd: "/z"}},
		{"missed", &v1, &v2, Notification{Type: NotificationsMissed, VersionId: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordsClient{records: map[string][]byte{}}
			client.write("/key", test.before)

			c := loadRacing(t, client, func(c *cacheImpl[string]) {
				client.write("/key", test.after)
				c.handleNotification(&test.notification)
			})
			assertRecord(t, c, test.after)
		})
	}
}

// Nor must a Put or a Delete through the cache.
func TestCache_LoadRacingWrite(t *testing.T) {
	v1, v2 := "v1", "v2"

	t.Run("put", func(t *testing.T) {
		client := &recordsClient{records: map[string][]byte{}}
		client.write("/key", &v1)

		c := loadRacing(t, client, func(c *cacheImpl[string]) {
			_, _, err := c.Put(context.Background(), "/key", v2)
			assert.NoError(t, err)
		})
		assertRecord(t, c, &v2)
	})

	t.Run("delete", func(t *testing.T) {
		client := &recordsClient{records: map[string][]byte{}}
		client.write("/key", &v1)

		c := loadRacing(t, client, func(c *cacheImpl[string]) {
			assert.NoError(t, c.Delete(context.Background(), "/key"))
		})
		assertRecord(t, c, nil)
	})
}

// The notification of another key must not keep a load from caching the record.
func TestCache_LoadRacingOtherKeyNotification(t *testing.T) {
	v1 := "v1"
	client := &recordsClient{records: map[string][]byte{}}
	client.write("/key", &v1)

	c := loadRacing(t, client, func(c *cacheImpl[string]) {
		client.write("/other", &v1)
		c.handleNotification(&Notification{Type: KeyCreated, Key: "/other"})
	})
	assertRecord(t, c, &v1)
	assert.EqualValues(t, 1, client.gets.Load(), "the second Get read the server")
}
