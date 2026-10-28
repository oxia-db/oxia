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

package perf

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func TestNewFailsWhenServerIsUnreachable(t *testing.T) {
	config := testConfig("localhost:1", false)

	_, err := New(config)

	assert.ErrorContains(t, err, "failed to create Oxia client")
}

func TestPerfRunWritesZeroPayloads(t *testing.T) {
	server := newStandalone(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, newPerf(t, testConfig(server.ServiceAddr(), false)))

	client := newSyncClient(t, server.ServiceAddr())
	var keys []string
	assert.Eventually(t, func() bool {
		var err error
		keys, err = client.List(context.Background(), "key-", "key-~") //nolint:staticcheck
		return err == nil && len(keys) > 0
	}, 10*time.Second, 10*time.Millisecond)

	cancel()
	<-done
	_, value, _, err := client.Get(context.Background(), keys[0])
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 16), value)
}

func TestPerfRunCountsFailedOperations(t *testing.T) {
	// Closed by the test itself: Standalone.Close panics when called twice
	server, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	p := newPerf(t, testConfig(server.ServiceAddr(), false)).(*perf)
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, p)

	client := newSyncClient(t, server.ServiceAddr())
	assert.Eventually(t, func() bool {
		keys, err := client.List(context.Background(), "key-", "key-~") //nolint:staticcheck
		return err == nil && len(keys) > 0
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, server.Close())

	assert.Eventually(t, func() bool {
		return p.failedOps.Load() > 0
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	<-done
}

func TestPerfRunWritesRandomPayloads(t *testing.T) {
	server := newStandalone(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, newPerf(t, testConfig(server.ServiceAddr(), true)))

	client := newSyncClient(t, server.ServiceAddr())
	assert.Eventually(t, func() bool {
		keys, err := client.List(context.Background(), "key-", "key-~") //nolint:staticcheck
		return err == nil && len(keys) > 0
	}, 10*time.Second, 10*time.Millisecond)

	cancel()
	<-done
	keys, err := client.List(context.Background(), "key-", "key-~") //nolint:staticcheck
	require.NoError(t, err)
	_, value, _, err := client.Get(context.Background(), keys[0])
	require.NoError(t, err)
	assert.Len(t, value, 16)
	assert.NotEqual(t, make([]byte, 16), value)
}

func TestPerfRunReportsStats(t *testing.T) {
	server := newStandalone(t)
	logs := captureLogs(t)
	config := testConfig(server.ServiceAddr(), false)
	config.StatsInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := runInBackground(ctx, newPerf(t, config))

	assert.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "Stats - Total ops")
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	<-done
}

func TestPerfRunLogsCloseError(t *testing.T) {
	logs := captureLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &perf{config: testConfig("", false), client: &fakeAsyncClient{closeErr: errors.New("close failed")}}

	p.Run(ctx)

	assert.Contains(t, logs.String(), "Failed to close Oxia client")
	assert.Contains(t, logs.String(), "close failed")
}

func testConfig(serviceAddr string, randomPayload bool) Config {
	return Config{
		ServiceAddr:         serviceAddr,
		Namespace:           oxia.DefaultNamespace,
		RequestRate:         200,
		ReadPercentage:      50,
		KeysCardinality:     10,
		ValueSize:           16,
		RandomPayload:       randomPayload,
		BatchLinger:         time.Millisecond,
		MaxRequestsPerBatch: 10,
		RequestTimeout:      200 * time.Millisecond,
	}
}

func newStandalone(t *testing.T) *dataserver.Standalone {
	t.Helper()
	server, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func newSyncClient(t *testing.T, serviceAddr string) oxia.SyncClient {
	t.Helper()
	client, err := oxia.NewSyncClient(serviceAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newPerf(t *testing.T, config Config) Perf {
	t.Helper()
	p, err := New(config)
	require.NoError(t, err)
	return p
}

func captureLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	buffer := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buffer
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func runInBackground(ctx context.Context, p Perf) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	return done
}

type fakeAsyncClient struct {
	oxia.AsyncClient
	closeErr error
}

func (c *fakeAsyncClient) Close() error {
	return c.closeErr
}
