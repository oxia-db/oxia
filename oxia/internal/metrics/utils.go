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
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	ometric "github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
)

func newHistogram(meter metric.Meter, name string, unit ometric.Unit) metric.Int64Histogram {
	histogram, err := meter.Int64Histogram(name, metric.WithUnit(string(unit)))
	fatalOnErr(err, name)
	return histogram
}

func newMillisCounter(meter metric.Meter, name string) metric.Float64Counter {
	counter, err := meter.Float64Counter(name, metric.WithUnit(string(ometric.Milliseconds)))
	fatalOnErr(err, name)
	return counter
}

func newCounter(meter metric.Meter, name string, unit ometric.Unit) metric.Int64Counter {
	counter, err := meter.Int64Counter(name, metric.WithUnit(string(unit)))
	fatalOnErr(err, name)
	return counter
}

func fatalOnErr(err error, name string) {
	if err != nil {
		slog.Error(
			"Failed to create metric",
			slog.Any("error", err),
			slog.String("component", "oxia-client"),
			slog.String("MetricName", name),
		)
		os.Exit(1)
	}
}

func attrs(requestType string, err error) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.Key("type").String(requestType),
		attribute.Key("result").String(result(err)),
	)
}

func result(err error) string {
	if err == nil {
		return "success"
	}
	return "failure"
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
