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

package metric

import (
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// withSDKMeter swaps in an SDK meter for the duration of the benchmark: the
// default global meter is a no-op, which would measure nothing. Each run gets
// its own meter, hence fresh series: counters size their stripes when they
// first see contention, and with -cpu the sub-benchmarks are re-run under
// another GOMAXPROCS.
func withSDKMeter(b *testing.B) {
	b.Helper()
	previous := GetMeter()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewManualReader()),
		sdkmetric.WithCardinalityLimit(0))
	SetMeter(provider.Meter("bench"))
	b.Cleanup(func() {
		SetMeter(previous)
		_ = provider.Shutdown(b.Context())
	})
}

// benchLabels mirrors the widest label sets on the dataserver hot paths
// (e.g. the per-follower cursor metrics).
var benchLabels = map[string]any{
	"oxia_namespace": "default",
	"shard":          int64(1),
	"follower":       "oxia-2.oxia-svc.oxia.svc.cluster.local:6649",
	"type":           "write",
}

func BenchmarkCounter(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		withSDKMeter(b)
		c := NewCounter("bench_counter", "", Dimensionless, benchLabels)
		b.ReportAllocs()
		for b.Loop() {
			c.Inc()
		}
	})
	b.Run("parallel", func(b *testing.B) {
		withSDKMeter(b)
		c := NewCounter("bench_counter", "", Dimensionless, benchLabels)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Inc()
			}
		})
	})
}

func BenchmarkUpDownCounter(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		withSDKMeter(b)
		c := NewUpDownCounter("bench_up_down_counter", "", Dimensionless, benchLabels)
		b.ReportAllocs()
		for b.Loop() {
			c.Add(1)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		withSDKMeter(b)
		c := NewUpDownCounter("bench_up_down_counter", "", Dimensionless, benchLabels)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
			}
		})
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		withSDKMeter(b)
		h := NewBytesHistogram("bench_histogram", "", benchLabels)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			h.Record(i & 0xffff)
			i++
		}
	})
	b.Run("parallel", func(b *testing.B) {
		withSDKMeter(b)
		h := NewBytesHistogram("bench_histogram", "", benchLabels)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Record(i & 0xffff)
				i++
			}
		})
	})
}

func BenchmarkLatencyHistogram(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		withSDKMeter(b)
		h := NewLatencyHistogram("bench_latency_histogram", "", benchLabels)
		b.ReportAllocs()
		for b.Loop() {
			h.Timer().Done()
		}
	})
	b.Run("parallel", func(b *testing.B) {
		withSDKMeter(b)
		h := NewLatencyHistogram("bench_latency_histogram", "", benchLabels)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				h.Timer().Done()
			}
		})
	})
}
