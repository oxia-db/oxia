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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type neverCompleteAsyncClient struct {
}

func (c *neverCompleteAsyncClient) Close() error { return nil }

func (c *neverCompleteAsyncClient) Put(key string, value []byte, options ...PutOption) <-chan PutResult {
	return make(chan PutResult)
}

func (c *neverCompleteAsyncClient) Delete(key string, options ...DeleteOption) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) Get(key string, options ...GetOption) <-chan GetResult {
	return make(chan GetResult)
}

func (c *neverCompleteAsyncClient) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...ListOption) <-chan ListResult {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) RangeScan(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...RangeScanOption) <-chan GetResult {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) GetNotifications() (Notifications, error) {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) GetSequenceUpdates(ctx context.Context, prefixKey string, options ...GetSequenceUpdatesOption) (<-chan string, error) {
	panic("not implemented")
}

func TestCancelContext(t *testing.T) {
	_asyncClient := &neverCompleteAsyncClient{}
	syncClient := newSyncClient(_asyncClient)

	assertCancellable(t, func(ctx context.Context) error {
		_, _, err := syncClient.Put(ctx, "/a", []byte{})
		return err
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.Delete(ctx, "/a")
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.DeleteRange(ctx, "/a", "/b")
	})
	assertCancellable(t, func(ctx context.Context) error {
		_, _, _, err := syncClient.Get(ctx, "/a")
		return err
	})

	err := syncClient.Close()
	assert.NoError(t, err)
}

func assertCancellable(t *testing.T, operationFunc func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error)
	go func() {
		errCh <- operationFunc(ctx)
	}()

	cancel()

	assert.ErrorIs(t, <-errCh, context.Canceled)
}
