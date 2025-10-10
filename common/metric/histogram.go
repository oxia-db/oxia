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

package metric

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

var sizeBucketsBytes = []float64{
	0x10, 0x20, 0x40, 0x80,
	0x100, 0x200, 0x400, 0x800,
	0x1000, 0x2000, 0x4000, 0x8000,
	0x10000, 0x20000, 0x40000, 0x80000,
	0x100000, 0x200000, 0x400000, 0x800000,
}

var sizeBucketsCount = []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 10_000, 20_000, 50_000, 100_000, 1_000_000}

type Histogram interface {
	Record(value int)
}

type histogram struct {
	h     metric.Int64Histogram
	attrs metric.MeasurementOption
}

func (t *histogram) Record(size int) {
	t.h.Record(context.Background(), int64(size), t.attrs)
}

func NewCountHistogram(name string, description string, labels map[string]any) Histogram {
	return newHistogram(name, Dimensionless, description, labels)
}

func NewBytesHistogram(name string, description string, labels map[string]any) Histogram {
	return newHistogram(name, Bytes, description, labels)
}

func newHistogram(name string, unit Unit, description string, labels map[string]any) Histogram {
	h, err := meter.Int64Histogram(
		name,
		metric.WithUnit(string(unit)),
		metric.WithDescription(description),
	)
	fatalOnErr(err, name)

	return &histogram{h: h, attrs: getAttrs(labels)}
}
