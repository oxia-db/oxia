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

// Counter is a monotonically increasing counter.
type Counter interface {
	Inc()
	Add(incr int)
}

type counter struct {
	sc    metric.Int64Counter
	attrs metric.MeasurementOption
}

func (c *counter) Inc() {
	c.Add(1)
}

func (c *counter) Add(incr int) {
	c.sc.Add(context.Background(), int64(incr), c.attrs)
}

func NewCounter(name string, description string, unit Unit, labels map[string]any) Counter {
	sc, err := meter.Int64Counter(name,
		metric.WithUnit(string(unit)),
		metric.WithDescription(description))
	fatalOnErr(err, name)
	return &counter{
		sc:    sc,
		attrs: getAttrs(labels),
	}
}

// UpDownCounter is a counter that is incremented and decremented
// to report the current state.
type UpDownCounter interface {
	Counter
	Dec()
	Sub(diff int)
}

type upDownCounter struct {
	sc    metric.Int64UpDownCounter
	attrs metric.MeasurementOption
}

func (c *upDownCounter) Inc() {
	c.Add(1)
}

func (c *upDownCounter) Add(incr int) {
	c.sc.Add(context.Background(), int64(incr), c.attrs)
}

func (c *upDownCounter) Dec() {
	c.Add(-1)
}

func (c *upDownCounter) Sub(diff int) {
	c.Add(-diff)
}

func NewUpDownCounter(name string, description string, unit Unit, labels map[string]any) UpDownCounter {
	sc, err := meter.Int64UpDownCounter(name,
		metric.WithUnit(string(unit)),
		metric.WithDescription(description))
	fatalOnErr(err, name)
	return &upDownCounter{
		sc:    sc,
		attrs: getAttrs(labels),
	}
}
