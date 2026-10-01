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

package dataserver

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

const (
	e2eBenchKeys      = 10_000
	e2eBenchServers   = 3
	e2eBenchValueSize = 10
	// Enough for the 20% of writes in Mixed80Read to fill the client batches
	// (oxia.DefaultMaxRequestsPerBatch), so they're closed on size and not on linger.
	e2eBenchMaxInFlight = 5_000
)

// BenchmarkE2E drives an in-process cluster (a coordinator and e2eBenchServers
// data servers, with a single shard replicated on all of them) through the async
// client, keeping up to e2eBenchMaxInFlight operations outstanding, so that
// before/after runs can be compared with benchstat (see dev/bench-compare.sh).
// Each workload runs against a fresh cluster, so the background work left by one
// (flushes, compactions) doesn't bleed into the next. The WAL fsync is disabled to keep the
// numbers sensitive to CPU and allocation changes rather than to the disk.
func BenchmarkE2E(b *testing.B) {
	// The logs go to stdout, where they'd break the result lines. Starting and
	// stopping a cluster always logs warnings and errors (e.g. the replication
	// streams cut at close), while a failed operation fails the benchmark anyway.
	logging.LogLevel = slog.LevelError + 1
	logging.ConfigureLogger()

	b.Run("Put", func(b *testing.B) {
		e := newE2EBench(b)
		e.run(b, e.put)
	})
	b.Run("Get", func(b *testing.B) {
		e := newE2EBench(b)
		e.run(b, e.get)
	})
	b.Run("Mixed80Read", func(b *testing.B) {
		e := newE2EBench(b)
		r := rand.New(rand.NewPCG(2, 2))
		e.run(b, func(key string) func() error {
			if r.IntN(100) < 80 {
				return e.get(key)
			}
			return e.put(key)
		})
	})
}

type e2eBench struct {
	client oxia.AsyncClient
	keys   []string
	value  []byte
}

// newE2EBench starts a cluster with all the keys loaded, and closes it when the
// benchmark function returns.
func newE2EBench(b *testing.B) *e2eBench {
	b.Helper()
	tmp := b.TempDir()

	servers := make([]string, e2eBenchServers)
	var serviceAddr string
	for i := range servers {
		options := option.NewDefaultOptions()
		options.Server.Public.BindAddress = "localhost:0"
		options.Server.Internal.BindAddress = "localhost:0"
		options.Observability.Metric.Enabled = &constant.FlagFalse
		options.Storage.Database.Dir = fmt.Sprintf("%s/s%d/db", tmp, i)
		options.Storage.WAL.Dir = fmt.Sprintf("%s/s%d/wal", tmp, i)
		options.Storage.WAL.Sync = &constant.FlagFalse

		server, err := New(context.Background(), commonwatch.New(options))
		require.NoError(b, err)
		b.Cleanup(func() { require.NoError(b, server.Close()) })

		serviceAddr = fmt.Sprintf("localhost:%d", server.PublicPort())
		servers[i] = fmt.Sprintf("  - name: s%d\n    public: %s\n    internal: localhost:%d\n",
			i, serviceAddr, server.InternalPort())
	}

	coordinatorDir := tmp + "/coordinator"
	require.NoError(b, os.MkdirAll(coordinatorDir, 0o755))
	clusterConfig := fmt.Sprintf("namespaces:\n  - name: %s\n    initialShardCount: 1\n    replicationFactor: %d\n"+
		"servers:\n%s", constant.DefaultNamespace, e2eBenchServers, strings.Join(servers, ""))
	require.NoError(b, os.WriteFile(filepath.Join(coordinatorDir, coordoption.DefaultFileConfigName),
		[]byte(clusterConfig), 0o600))

	coordinatorOptions := coordoption.NewDefaultOptions()
	coordinatorOptions.Server.Public.BindAddress = "localhost:0"
	coordinatorOptions.Server.Internal.BindAddress = "localhost:0"
	coordinatorOptions.Observability.Metric.Enabled = &constant.FlagFalse
	coordinatorOptions.Metadata.ProviderName = metadatacommon.NameFile
	coordinatorOptions.Metadata.File.Dir = coordinatorDir
	coord, err := coordinator.NewGrpcServer(context.Background(), commonwatch.New(coordinatorOptions))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, coord.Close()) })

	// The first puts retry until the shard has elected a leader.
	client, err := oxia.NewAsyncClient(serviceAddr)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, client.Close()) })

	e := &e2eBench{
		client: client,
		keys:   make([]string, e2eBenchKeys),
		value:  make([]byte, e2eBenchValueSize),
	}
	loaded := make([]<-chan oxia.PutResult, e2eBenchKeys)
	for i := range e.keys {
		e.keys[i] = fmt.Sprintf("key-%d", i)
		loaded[i] = client.Put(e.keys[i], e.value)
	}
	for _, ch := range loaded {
		require.NoError(b, (<-ch).Err)
	}
	return e
}

func (e *e2eBench) put(key string) func() error {
	ch := e.client.Put(key, e.value)
	return func() error { return (<-ch).Err }
}

func (e *e2eBench) get(key string) func() error {
	ch := e.client.Get(key)
	return func() error { return (<-ch).Err }
}

// run issues b.N operations from a single goroutine, each returning a function
// that waits for its result. It reports ns/op as the inverse of the aggregate
// throughput, plus the per-operation latency percentiles.
func (e *e2eBench) run(b *testing.B, op func(key string) func() error) {
	b.Helper()
	r := rand.New(rand.NewPCG(1, 1))
	latencies := make([]time.Duration, b.N)
	inFlight := make(chan struct{}, e2eBenchMaxInFlight)
	var wg sync.WaitGroup

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		inFlight <- struct{}{}
		start := time.Now()
		wait := op(e.keys[r.IntN(len(e.keys))])
		wg.Go(func() {
			if err := wait(); err != nil {
				b.Error(err)
			}
			latencies[i] = time.Since(start)
			<-inFlight
		})
	}
	wg.Wait()
	b.StopTimer()

	slices.Sort(latencies)
	percentile := func(p float64) float64 {
		return float64(latencies[int(p*float64(len(latencies)-1))].Microseconds())
	}
	b.ReportMetric(percentile(0.50), "p50-µs")
	b.ReportMetric(percentile(0.99), "p99-µs")
}
