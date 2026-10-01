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
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Counter is a monotonically increasing counter.
type Counter interface {
	Inc()
	Add(incr int)
}

type counter struct {
	s *sumSeries
}

func (c *counter) Inc() {
	c.Add(1)
}

func (c *counter) Add(incr int) {
	c.s.add(int64(incr))
}

func NewCounter(name string, description string, unit Unit, labels map[string]any) Counter {
	return &counter{
		s: getObservedSum(sumID{name, description, unit, false}).series(labels),
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
	s *sumSeries
}

func (c *upDownCounter) Inc() {
	c.Add(1)
}

func (c *upDownCounter) Add(incr int) {
	c.s.add(int64(incr))
}

func (c *upDownCounter) Dec() {
	c.Add(-1)
}

func (c *upDownCounter) Sub(diff int) {
	c.Add(-diff)
}

func NewUpDownCounter(name string, description string, unit Unit, labels map[string]any) UpDownCounter {
	return &upDownCounter{
		s: getObservedSum(sumID{name, description, unit, true}).series(labels),
	}
}

// Counters are kept in atomics and exported through observable instruments:
// a synchronous OTel counter costs ~20x an atomic add on every call. The
// exported data is the same cumulative sum, as long as the observable side
// keeps the synchronous semantics:
//   - counters with the same identity and labels add into one series,
//   - a series is reported from its first measurement on, and never dropped.

// sumID is what the SDK identifies an instrument by, within a meter.
type sumID struct {
	name        string
	description string
	unit        Unit
	upDown      bool
}

// sumSeries is the cumulative value of one attribute set.
type sumSeries struct {
	value    atomic.Int64
	recorded atomic.Bool
	attrs    metric.MeasurementOption
}

func (s *sumSeries) add(n int64) {
	s.value.Add(n)
	if !s.recorded.Load() {
		s.recorded.Store(true)
	}
}

// observedSum is an observable instrument and all its series.
type observedSum struct {
	sync.Mutex
	byAttrs map[attribute.Distinct]*sumSeries
}

func (o *observedSum) series(labels map[string]any) *sumSeries {
	set := getAttrSet(labels)
	o.Lock()
	defer o.Unlock()
	s, ok := o.byAttrs[set.Equivalent()]
	if !ok {
		s = &sumSeries{attrs: metric.WithAttributeSet(set)}
		o.byAttrs[set.Equivalent()] = s
	}
	return s
}

// observe runs under the SDK collection lock: it must not block on anything
// but the instrument's own map.
func (o *observedSum) observe(_ context.Context, obs metric.Int64Observer) error {
	o.Lock()
	defer o.Unlock()
	for _, s := range o.byAttrs {
		if s.recorded.Load() {
			obs.Observe(s.value.Load(), s.attrs)
		}
	}
	return nil
}

var (
	observedSumsLock sync.Mutex
	// observedSums holds the instruments of the current meter.
	observedSums = map[sumID]*observedSum{}
)

func getObservedSum(id sumID) *observedSum {
	observedSumsLock.Lock()
	defer observedSumsLock.Unlock()
	if o, ok := observedSums[id]; ok {
		return o
	}

	o := &observedSum{byAttrs: map[attribute.Distinct]*sumSeries{}}
	unit := metric.WithUnit(string(id.unit))
	description := metric.WithDescription(id.description)
	callback := metric.WithInt64Callback(o.observe)
	var err error
	if id.upDown {
		_, err = GetMeter().Int64ObservableUpDownCounter(id.name, unit, description, callback)
	} else {
		_, err = GetMeter().Int64ObservableCounter(id.name, unit, description, callback)
	}
	fatalOnErr(err, id.name)
	observedSums[id] = o
	return o
}
