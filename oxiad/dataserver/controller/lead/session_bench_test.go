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

package lead

import (
	"context"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

// BenchmarkSessionFootprint measures the resident memory cost of holding live
// sessions (see https://github.com/oxia-db/oxia/issues/1235). Run with a fixed
// iteration count so the reported per-session numbers correspond to a known
// population, e.g.:
//
//	go test -bench BenchmarkSessionFootprint -benchtime=100000x -run NONE ./oxiad/dataserver/controller/lead/
func BenchmarkSessionFootprint(b *testing.B) {
	sm := &sessionManager{
		sessions: make(map[SessionId]*session),
		log:      slog.Default(),
		activeSessions: metric.NewUpDownCounter("oxia_bench_active_sessions",
			"bench", "count", map[string]any{}),
	}
	sm.ctx, sm.cancel = context.WithCancel(context.Background())

	metadata := &proto.SessionMetadata{
		TimeoutMs: uint32((10 * time.Minute).Milliseconds()),
		Identity:  "bench-client-identity",
	}

	goroutinesBefore := runtime.NumGoroutine()
	runtime.GC() //nolint:revive // settle the heap so the before/after delta only measures sessions
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ResetTimer()
	sm.Lock()
	for i := 0; i < b.N; i++ {
		startSession(SessionId(i), metadata, sm)
	}
	sm.Unlock()
	b.StopTimer()

	runtime.GC() //nolint:revive // drop creation-time garbage, keeping only the live per-session state
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapPerSession := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(b.N)
	stackPerSession := float64(int64(after.StackInuse)-int64(before.StackInuse)) / float64(b.N)
	b.ReportMetric(heapPerSession, "heap-B/session")
	b.ReportMetric(stackPerSession, "stack-B/session")
	b.ReportMetric(heapPerSession+stackPerSession, "total-B/session")
	b.ReportMetric(float64(runtime.NumGoroutine()-goroutinesBefore)/float64(b.N), "goroutines/session")

	if err := sm.Close(); err != nil {
		b.Fatal(err)
	}
}
