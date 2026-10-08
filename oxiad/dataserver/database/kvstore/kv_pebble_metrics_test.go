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

package kvstore

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

func TestPebbleReadWriteOpsMetrics(t *testing.T) {
	// Swap in an SDK meter so the counter values can be read back, and read
	// the histograms through their producer, as the Prometheus exporter does.
	previous := metric.GetMeter()
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(metric.HistogramProducer))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metric.SetMeter(provider.Meter("test"))
	defer metric.SetMeter(previous)

	readCounter := func(name string) int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != name {
					continue
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "unexpected data for %s: %#v", name, m.Data)
				var total int64
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
				return total
			}
		}
		return 0
	}

	readHistogramCount := func(name string) uint64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != name {
					continue
				}
				histo, ok := m.Data.(metricdata.Histogram[float64])
				require.True(t, ok, "unexpected data for %s: %#v", name, m.Data)
				var total uint64
				for _, dp := range histo.DataPoints {
					total += dp.Count
				}
				return total
			}
		}
		return 0
	}

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer factory.Close()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	defer kv.Close()

	writesBefore := readCounter("oxia_server_kv_write_ops")
	readsBefore := readCounter("oxia_server_kv_read_ops")
	readBytesBefore := readCounter("oxia_server_kv_read")
	readLatencyBefore := readHistogramCount("oxia_server_kv_read_latency")

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	for _, key := range []string{"a", "b", "c", "a"} {
		_, _, closer, err := kv.Get(key, ComparisonEqual, NoInternalKeys)
		require.NoError(t, err)
		assert.NoError(t, closer.Close())
	}
	_, _, _, err = kv.Get("non-existing", ComparisonEqual, NoInternalKeys)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	assert.EqualValues(t, 3, readCounter("oxia_server_kv_write_ops")-writesBefore)
	assert.EqualValues(t, 5, readCounter("oxia_server_kv_read_ops")-readsBefore)
	assert.EqualValues(t, 4, readCounter("oxia_server_kv_read")-readBytesBefore)
	assert.EqualValues(t, 5, readHistogramCount("oxia_server_kv_read_latency")-readLatencyBefore)
}

func TestPebbleWriteStallMetrics(t *testing.T) {
	previous := metric.GetMeter()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metric.SetMeter(provider.Meter("test"))
	defer metric.SetMeter(previous)

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer factory.Close()
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	defer kv.Close()

	// A flush adds a sublevel to L0. The gauges cache the db metrics for a
	// while: nothing may read them before the flush
	wb := kv.NewWriteBatch()
	require.NoError(t, wb.Put("a", []byte("0")))
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())
	require.NoError(t, kv.Flush())

	// A stall of each kind, through a listener on the counters of the db
	listener := kv.(*Pebble).writeStallListener()
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: "memtable count limit reached"})
	time.Sleep(20 * time.Millisecond)
	listener.WriteStallEnd()
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: "L0 file count limit exceeded"})
	listener.WriteStallEnd()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	// value sums the points of the metric, of the given reason if not empty
	value := func(name string, reason string) int64 {
		var total int64
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != name {
					continue
				}
				var points []metricdata.DataPoint[int64]
				switch data := m.Data.(type) {
				case metricdata.Sum[int64]:
					points = data.DataPoints
				case metricdata.Gauge[int64]:
					points = data.DataPoints
				default:
					require.Failf(t, "unexpected data", "%s: %#v", name, m.Data)
				}
				for _, dp := range points {
					if v, ok := dp.Attributes.Value(attribute.Key("reason")); reason == "" || ok && v.AsString() == reason {
						total += dp.Value
					}
				}
			}
		}
		return total
	}

	assert.EqualValues(t, 1, value("oxia_server_kv_pebble_write_stalls", "memtable"))
	assert.EqualValues(t, 1, value("oxia_server_kv_pebble_write_stalls", "l0"))
	assert.GreaterOrEqual(t, value("oxia_server_kv_pebble_write_stall_time", ""), int64(20))
	assert.EqualValues(t, 1, value("oxia_server_kv_pebble_l0_sublevels", ""))
}
