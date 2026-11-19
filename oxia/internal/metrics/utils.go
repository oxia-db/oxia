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

package metrics

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	ometric "github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

func newHistogram(meter metric.Meter, name string, unit ometric.Unit) metric.Int64Histogram {
	histogram, err := meter.Int64Histogram(name, metric.WithUnit(string(unit)))
	panicOnErr(err, name)
	return histogram
}

func newMillisCounter(meter metric.Meter, name string) metric.Float64Counter {
	counter, err := meter.Float64Counter(name, metric.WithUnit(string(ometric.Milliseconds)))
	panicOnErr(err, name)
	return counter
}

func newCounter(meter metric.Meter, name string, unit ometric.Unit) metric.Int64Counter {
	counter, err := meter.Int64Counter(name, metric.WithUnit(string(unit)))
	panicOnErr(err, name)
	return counter
}

// panicOnErr fails the creation of a client metric. The OTel SDK only fails it
// for a programming error, e.g. an invalid name, which a caller cannot handle.
func panicOnErr(err error, name string) {
	if err != nil {
		panic(fmt.Errorf("failed to create oxia client metric %s: %w", name, err))
	}
}

const (
	resultSuccess = "success"
	resultFailure = "failure"
)

func attrs(requestType string, result string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.Key("type").String(requestType),
		attribute.Key("result").String(result),
	)
}

func writeMetrics(request *proto.WriteRequest) (valueSize int64, requestCount int64) {
	for _, put := range request.Puts {
		valueSize += int64(len(put.Value))
	}
	requestCount = int64(len(request.Puts) + len(request.Deletes) + len(request.DeleteRanges))
	return valueSize, requestCount
}

func readMetrics(response *proto.ReadResponse) (valueSize int64, requestCount int64) {
	if response == nil {
		return 0, 0
	}

	for _, get := range response.Gets {
		valueSize += int64(len(get.Value))
	}
	requestCount = int64(len(response.Gets))
	return valueSize, requestCount
}
