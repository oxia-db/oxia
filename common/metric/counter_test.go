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
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

// syncCounter is the counter as it was before it moved to atomics: a
// synchronous SDK instrument, which the exported data must stay equal to.
type syncCounter struct {
	add   func(ctx context.Context, incr int64, options ...metric.AddOption)
	attrs metric.MeasurementOption
}

func (c *syncCounter) Inc()         { c.Add(1) }
func (c *syncCounter) Add(incr int) { c.add(context.Background(), int64(incr), c.attrs) }
func (c *syncCounter) Dec()         { c.Add(-1) }
func (c *syncCounter) Sub(diff int) { c.Add(-diff) }

type counterFactory struct {
	newCounter       func(name string, description string, unit Unit, labels map[string]any) Counter
	newUpDownCounter func(name string, description string, unit Unit, labels map[string]any) UpDownCounter
}

func syncCounterFactory(m metric.Meter) counterFactory {
	return counterFactory{
		newCounter: func(name string, description string, unit Unit, labels map[string]any) Counter {
			c, err := m.Int64Counter(name, metric.WithUnit(string(unit)), metric.WithDescription(description))
			fatalOnErr(err, name)
			return &syncCounter{add: c.Add, attrs: getAttrs(labels)}
		},
		newUpDownCounter: func(name string, description string, unit Unit, labels map[string]any) UpDownCounter {
			c, err := m.Int64UpDownCounter(name, metric.WithUnit(string(unit)), metric.WithDescription(description))
			fatalOnErr(err, name)
			return &syncCounter{add: c.Add, attrs: getAttrs(labels)}
		},
	}
}

// counterScenario exercises what the exported data depends on, and returns
// the data collected after each step.
func counterScenario(t *testing.T, f counterFactory, reader sdkmetric.Reader) []metricdata.ScopeMetrics {
	t.Helper()
	shard1 := LabelsForShard("default", 1)
	shard2 := LabelsForShard("default", 2)

	collect := func() metricdata.ScopeMetrics {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		if len(rm.ScopeMetrics) == 0 {
			return metricdata.ScopeMetrics{}
		}
		require.Len(t, rm.ScopeMetrics, 1)
		return rm.ScopeMetrics[0]
	}

	// A counter that was never touched has no series.
	ops := f.newCounter("ops", "ops", Dimensionless, shard1)
	f.newCounter("ops", "ops", Dimensionless, shard2)
	f.newUpDownCounter("active", "active", Dimensionless, shard1)
	f.newCounter("untouched", "untouched", Dimensionless, shard1)
	var steps []metricdata.ScopeMetrics
	steps = append(steps, collect())

	ops.Inc()
	ops.Add(5)
	// Same identity and labels: one series for both.
	f.newCounter("ops", "ops", Dimensionless, shard1).Add(2)
	// Same name, another unit: another instrument.
	f.newCounter("ops", "ops", Bytes, shard1).Add(100)
	// No labels.
	f.newCounter("global", "global", Dimensionless, map[string]any{}).Inc()
	active := f.newUpDownCounter("active", "active", Dimensionless, shard1)
	active.Inc()
	active.Inc()
	active.Dec()
	active.Sub(1)
	f.newUpDownCounter("active", "active", Dimensionless, shard2).Sub(3)
	steps = append(steps, collect())

	// Cumulative: values carry over across collections.
	ops.Add(10)
	steps = append(steps, collect())
	return steps
}

func TestCounter_ExportedDataUnchanged(t *testing.T) {
	syncReader := sdkmetric.NewManualReader()
	syncProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(syncReader))
	expected := counterScenario(t, syncCounterFactory(syncProvider.Meter("test")), syncReader)

	previous := GetMeter()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	SetMeter(provider.Meter("test"))
	defer SetMeter(previous)
	actual := counterScenario(t, counterFactory{NewCounter, NewUpDownCounter}, reader)

	require.Len(t, actual, len(expected))
	for i := range expected {
		metricdatatest.AssertEqual(t, expected[i], actual[i], metricdatatest.IgnoreTimestamp())
	}
	// Sanity check on the scenario itself.
	require.Empty(t, expected[0].Metrics)
	require.Len(t, expected[1].Metrics, 4)
}

// The SDK ignores the callbacks of an observable instrument created again on
// the same meter, so counters created after a meter is restored must keep
// adding into the series registered on it the first time.
func TestCounter_MeterRestored(t *testing.T) {
	previous := GetMeter()
	defer SetMeter(previous)

	reader := sdkmetric.NewManualReader()
	SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	restored := GetMeter()
	NewCounter("restored", "", Dimensionless, map[string]any{}).Inc()

	SetMeter(sdkmetric.NewMeterProvider().Meter("other"))
	NewCounter("restored", "", Dimensionless, map[string]any{}).Inc()

	SetMeter(restored)
	NewCounter("restored", "", Dimensionless, map[string]any{}).Add(5)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	sum, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.EqualValues(t, 6, sum.DataPoints[0].Value)
}
