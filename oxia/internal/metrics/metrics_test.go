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

package metrics

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oxia-db/oxia/oxia/internal/model"
	"github.com/oxia-db/oxia/proto"
)

func TestMetricsDecorate(t *testing.T) {
	putFunc := func(metrics *Metrics, err error) {
		metrics.DecoratePut(model.PutCall{Callback: func(*proto.PutResponse, error) {}, Value: []byte{0, 1, 2, 3, 4}}).
			Callback(&proto.PutResponse{}, err)
	}
	deleteFunc := func(metrics *Metrics, err error) {
		metrics.DecorateDelete(model.DeleteCall{Callback: func(*proto.DeleteResponse, error) {}}).
			Callback(&proto.DeleteResponse{}, err)
	}
	deleteRangeFunc := func(metrics *Metrics, err error) {
		metrics.DecorateDeleteRange(model.DeleteRangeCall{Callback: func(*proto.DeleteRangeResponse, error) {}}).
			Callback(&proto.DeleteRangeResponse{}, err)
	}
	getFunc := func(metrics *Metrics, err error) {
		metrics.DecorateGet(model.GetCall{Callback: func(*proto.GetResponse, error) {}}).
			Callback(&proto.GetResponse{Value: []byte{0, 1, 2, 3, 4}}, err)
	}

	for _, item := range []struct {
		testFunc     func(*Metrics, error)
		expectedType string
		hasHistogram bool
	}{
		{putFunc, "put", true},
		{deleteFunc, "delete", false},
		{deleteRangeFunc, "delete_range", false},
		{getFunc, "get", true},
	} {
		for _, condition := range []struct {
			err            error
			expectedResult string
		}{
			{nil, "success"},
			{io.EOF, "failure"},
		} {
			t.Run(item.expectedType+"_"+condition.expectedResult, func(t *testing.T) {
				ch := make(chan time.Duration, 1)
				ch <- 1 * time.Millisecond
				metrics, reader := setup(func(t time.Time) time.Duration {
					return <-ch
				})

				item.testFunc(metrics, condition.err)

				rm := metricdata.ResourceMetrics{}
				err := reader.Collect(context.Background(), &rm)
				assert.NoError(t, err)

				assertTimer(t, rm, "oxia_client_op", item.expectedType, condition.expectedResult)
				if item.hasHistogram {
					assertWithHistogram(t, rm, "oxia_client_op_value", 5, item.expectedType, condition.expectedResult)
				} else {
					assertWithoutHistogram(t, rm, "oxia_client_op_value")
				}
			})
		}
	}
}

func TestMetricsCallback(t *testing.T) {
	writeFunc := func(metrics *Metrics, err error) {
		metrics.WriteCallback()(time.Now(), &proto.WriteRequest{
			Puts: []*proto.PutRequest{{Value: []byte{0, 1, 2, 3, 4}}},
		}, &proto.WriteResponse{}, err)
	}
	readFunc := func(metrics *Metrics, err error) {
		metrics.ReadCallback()(time.Now(), &proto.ReadRequest{}, &proto.ReadResponse{
			Gets: []*proto.GetResponse{{Value: []byte{0, 1, 2, 3, 4}}},
		}, err)
	}

	for _, item := range []struct {
		testFunc     func(*Metrics, error)
		expectedType string
	}{
		{writeFunc, "write"},
		{readFunc, "read"},
	} {
		for _, condition := range []struct {
			err            error
			expectedResult string
		}{
			{nil, "success"},
			{io.EOF, "failure"},
		} {
			t.Run(item.expectedType+"_"+condition.expectedResult, func(t *testing.T) {
				ch := make(chan time.Duration, 2)
				ch <- 1 * time.Millisecond
				ch <- 1 * time.Millisecond
				metrics, reader := setup(func(t time.Time) time.Duration {
					return <-ch
				})

				item.testFunc(metrics, condition.err)

				rm := metricdata.ResourceMetrics{}
				err := reader.Collect(context.Background(), &rm)
				assert.NoError(t, err)

				assertTimer(t, rm, "oxia_client_batch_total", item.expectedType, condition.expectedResult)
				assertTimer(t, rm, "oxia_client_batch_exec", item.expectedType, condition.expectedResult)
				assertWithHistogram(t, rm, "oxia_client_batch_value", 5, item.expectedType, condition.expectedResult)
				assertWithHistogram(t, rm, "oxia_client_batch_request", 1, item.expectedType, condition.expectedResult)
			})
		}
	}
}

func setup(sinceFunc func(time.Time) time.Duration) (*Metrics, metric.Reader) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	metrics := newMetrics(provider, time.Now, sinceFunc)
	return metrics, reader
}

func assertTimer(t *testing.T, rm metricdata.ResourceMetrics, name string, expectedType string, expectedResult string) {
	t.Helper()

	sums, err := counter[float64](rm, name)
	assert.NoError(t, err)
	assertDataPoints(t, sums, float64(1), expectedType, expectedResult)

	counts, err := counter[int64](rm, name)
	assert.NoError(t, err)
	assertDataPoints(t, counts, int64(1), expectedType, expectedResult)
}

func assertWithHistogram(t *testing.T, rm metricdata.ResourceMetrics, name string, expectedSum int64, expectedType string, expectedResult string) {
	t.Helper()

	datapoints, err := histogram(rm, name)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(datapoints))
	histogram := datapoints[0]
	assert.Equal(t, expectedSum, histogram.Sum)
	assert.Equal(t, uint64(1), histogram.Count)
	assertAttributes(t, histogram.Attributes, expectedType, expectedResult)
}

func assertWithoutHistogram(t *testing.T, rm metricdata.ResourceMetrics, name string) {
	t.Helper()

	datapoints, err := histogram(rm, name)
	assert.Error(t, err)
	assert.Equal(t, 0, len(datapoints))
}

func counter[N int64 | float64](rm metricdata.ResourceMetrics, name string) ([]metricdata.DataPoint[N], error) {
	for _, m := range rm.ScopeMetrics[0].Metrics {
		if m.Name == name {
			if d, ok := m.Data.(metricdata.Sum[N]); ok {
				return d.DataPoints, nil
			}
		}
	}
	return nil, errors.New("not found")
}

func histogram(rm metricdata.ResourceMetrics, name string) ([]metricdata.HistogramDataPoint[int64], error) {
	for _, m := range rm.ScopeMetrics[0].Metrics {
		if m.Name == name {
			if d, ok := m.Data.(metricdata.Histogram[int64]); ok {
				return d.DataPoints, nil
			}
		}
	}
	return nil, errors.New("not found")
}

func assertDataPoints[N int64 | float64](
	t *testing.T,
	datapoints []metricdata.DataPoint[N],
	expectedValue N,
	expectedType string, expectedResult string) {
	t.Helper()

	assert.Equal(t, 1, len(datapoints))
	assert.Equal(t, expectedValue, datapoints[0].Value)
	assertAttributes(t, datapoints[0].Attributes, expectedType, expectedResult)
}

func assertAttributes(t *testing.T, attrs attribute.Set, expectedType string, expectedResult string) {
	t.Helper()

	assert.Equal(t, 2, attrs.Len())
	assertAttribute(t, attrs, "type", expectedType)
	assertAttribute(t, attrs, "result", expectedResult)
}
func assertAttribute(t *testing.T, attrs attribute.Set, key attribute.Key, expected string) {
	t.Helper()

	value, ok := attrs.Value(key)
	assert.True(t, ok)
	assert.Equal(t, expected, value.AsString())
}
