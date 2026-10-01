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
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

// histogramRecorder records the same values into the SDK histograms the
// wrappers used before (with the explicit-bucket views the server had), and
// into the wrappers' series.
type histogramRecorder struct {
	create func(name string, unit Unit, labels map[string]any)
	record func(name string, unit Unit, labels map[string]any, value float64, sumValue int64)
}

func sdkHistogramRecorder(t *testing.T, m metric.Meter) histogramRecorder {
	t.Helper()
	return histogramRecorder{create: func(name string, unit Unit, _ map[string]any) {
		var err error
		if unit == Milliseconds {
			_, err = m.Float64Histogram(name, metric.WithUnit(string(unit)), metric.WithDescription(name))
		} else {
			_, err = m.Int64Histogram(name, metric.WithUnit(string(unit)), metric.WithDescription(name))
		}
		require.NoError(t, err)
	}, record: func(name string, unit Unit, labels map[string]any, value float64, _ int64) {
		unitOpt, description := metric.WithUnit(string(unit)), metric.WithDescription(name)
		if unit == Milliseconds {
			h, err := m.Float64Histogram(name, unitOpt, description)
			require.NoError(t, err)
			h.Record(context.Background(), value, getAttrs(labels))
			return
		}
		h, err := m.Int64Histogram(name, unitOpt, description)
		require.NoError(t, err)
		h.Record(context.Background(), int64(value), getAttrs(labels))
	}}
}

func wrapperHistogramRecorder() histogramRecorder {
	series := func(name string, unit Unit, labels map[string]any) *histSeries {
		switch unit {
		case Milliseconds:
			return NewLatencyHistogram(name, name, labels).(*latencyHistogram).s
		case Bytes:
			return NewBytesHistogram(name, name, labels).(*histogram).s
		default:
			return NewCountHistogram(name, name, labels).(*histogram).s
		}
	}
	return histogramRecorder{create: func(name string, unit Unit, labels map[string]any) {
		series(name, unit, labels)
	}, record: func(name string, unit Unit, labels map[string]any, value float64, sumValue int64) {
		series(name, unit, labels).record(value, sumValue)
	}}
}

func histogramViews() sdkmetric.Option {
	view := func(unit Unit, bounds []float64) sdkmetric.View {
		return sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: string(unit)},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: bounds}})
	}
	return sdkmetric.WithView(
		view(Milliseconds, latencyBucketsMillis), view(Bytes, sizeBucketsBytes), view(Dimensionless, sizeBucketsCount))
}

// histogramScenario records values on and around the bucket bounds into
// histograms named with prefix, and returns the data collected after each
// step.
func histogramScenario(
	t *testing.T, prefix string, r histogramRecorder, collect func() metricdata.ScopeMetrics,
) []metricdata.ScopeMetrics {
	t.Helper()
	shard1 := LabelsForShard("default", 1)
	shard2 := LabelsForShard("default", 2)
	latency := func(name string, labels map[string]any, micros int64) {
		r.record(name, Milliseconds, labels, float64(micros)/1000.0, micros)
	}

	// A histogram, or a series, that was never recorded is not reported.
	r.create(prefix+"latency", Milliseconds, shard1)
	r.create(prefix+"bytes", Bytes, shard2)
	r.create(prefix+"untouched", Dimensionless, shard1)
	var steps []metricdata.ScopeMetrics
	steps = append(steps, collect())

	for _, micros := range []int64{0, 99, 100, 101, 1_000, 1_001, 49_999_999, 50_000_000, 50_000_001, 3_600_000_000} {
		latency(prefix+"latency", shard1, micros)
	}
	latency(prefix+"latency", shard2, 250)
	for _, size := range []float64{0, 1, 15, 16, 17, 0x800000, 0x800001, 1 << 30} {
		r.record(prefix+"bytes", Bytes, shard1, size, int64(size))
	}
	for _, count := range []float64{0, 1, 2, 5, 6, 1_000_000, 1_000_001} {
		r.record(prefix+"count", Dimensionless, map[string]any{}, count, int64(count))
	}
	steps = append(steps, collect())

	// Cumulative: values carry over across collections.
	latency(prefix+"latency", shard1, 12_345)
	r.record(prefix+"bytes", Bytes, shard1, 100, 100)
	steps = append(steps, collect())
	return steps
}

// withoutExtrema drops the min and max the SDK computes: the Prometheus
// exporter does not export them, and the producer does not compute them.
func withoutExtrema(sm metricdata.ScopeMetrics) metricdata.ScopeMetrics {
	for i, m := range sm.Metrics {
		switch data := m.Data.(type) {
		case metricdata.Histogram[int64]:
			for j := range data.DataPoints {
				data.DataPoints[j].Min, data.DataPoints[j].Max = metricdata.Extrema[int64]{}, metricdata.Extrema[int64]{}
			}
		case metricdata.Histogram[float64]:
			for j := range data.DataPoints {
				data.DataPoints[j].Min, data.DataPoints[j].Max = metricdata.Extrema[float64]{}, metricdata.Extrema[float64]{}
			}
		default:
			// Not a histogram: no extrema.
		}
		sm.Metrics[i] = m
	}
	return sm
}

// withSumsOf checks that the float sums of actual are within rounding of the
// ones of expected (the SDK adds up milliseconds, the producer microseconds),
// and then takes the expected ones.
func withSumsOf(t *testing.T, actual, expected metricdata.ScopeMetrics) metricdata.ScopeMetrics {
	t.Helper()
	type key struct {
		name  string
		attrs attribute.Distinct
	}
	sums := map[key]float64{}
	for _, m := range expected.Metrics {
		if data, ok := m.Data.(metricdata.Histogram[float64]); ok {
			for _, dp := range data.DataPoints {
				sums[key{m.Name, dp.Attributes.Equivalent()}] = dp.Sum
			}
		}
	}
	for _, m := range actual.Metrics {
		if data, ok := m.Data.(metricdata.Histogram[float64]); ok {
			for j, dp := range data.DataPoints {
				expectedSum, ok := sums[key{m.Name, dp.Attributes.Equivalent()}]
				require.True(t, ok)
				require.InEpsilon(t, expectedSum, dp.Sum, 1e-12)
				data.DataPoints[j].Sum = expectedSum
			}
		}
	}
	return actual
}

var histogramTestRun atomic.Int64

func TestHistogram_ExportedDataUnchanged(t *testing.T) {
	// The histograms registry is global: the names must be new on each run
	// (e.g. with -count), for the first collection to be empty.
	prefix := fmt.Sprintf("histo_test_%d_", histogramTestRun.Add(1))
	sdkReader := sdkmetric.NewManualReader()
	sdkProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkReader), histogramViews())
	expected := histogramScenario(t, prefix, sdkHistogramRecorder(t, sdkProvider.Meter(meterName)), func() metricdata.ScopeMetrics {
		var rm metricdata.ResourceMetrics
		require.NoError(t, sdkReader.Collect(context.Background(), &rm))
		if len(rm.ScopeMetrics) == 0 {
			return metricdata.ScopeMetrics{}
		}
		require.Len(t, rm.ScopeMetrics, 1)
		return withoutExtrema(rm.ScopeMetrics[0])
	})

	// A reader with the producer, as the Prometheus exporter has.
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(HistogramProducer))
	sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	actual := histogramScenario(t, prefix, wrapperHistogramRecorder(), func() metricdata.ScopeMetrics {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		var sm metricdata.ScopeMetrics
		for _, s := range rm.ScopeMetrics {
			for _, m := range s.Metrics {
				// Only the histograms of this test: the registry is global.
				if strings.HasPrefix(m.Name, prefix) {
					sm.Scope = s.Scope
					sm.Metrics = append(sm.Metrics, m)
				}
			}
		}
		return sm
	})

	require.Len(t, actual, len(expected))
	for i := range expected {
		metricdatatest.AssertEqual(t, expected[i], withSumsOf(t, actual[i], expected[i]), metricdatatest.IgnoreTimestamp())
	}
	// Sanity check on the scenario itself.
	require.Empty(t, expected[0].Metrics)
	require.Len(t, expected[1].Metrics, 3)
}
