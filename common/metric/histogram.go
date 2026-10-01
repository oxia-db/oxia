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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var latencyBucketsMillis = []float64{
	0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000,
}

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
	s *histSeries
}

func (h *histogram) Record(value int) {
	h.s.record(float64(value), int64(value))
}

func NewCountHistogram(name string, description string, labels map[string]any) Histogram {
	return newHistogram(name, Dimensionless, description, labels)
}

func NewBytesHistogram(name string, description string, labels map[string]any) Histogram {
	return newHistogram(name, Bytes, description, labels)
}

func newHistogram(name string, unit Unit, description string, labels map[string]any) Histogram {
	bounds := sizeBucketsCount
	if unit == Bytes {
		bounds = sizeBucketsBytes
	}
	return &histogram{
		s: getHistogram(histID{name, description, unit}, bounds, 1).series(labels),
	}
}

type Timer struct {
	histo *latencyHistogram
	start time.Time
}

func (tm Timer) Done() {
	micros := time.Since(tm.start).Microseconds()
	tm.histo.s.record(float64(micros)/1000.0, micros)
}

// DoneCtx is Done: the context is not used, since nothing records exemplars.
func (tm Timer) DoneCtx(context.Context) {
	tm.Done()
}

type LatencyHistogram interface {
	Timer() Timer
}

type latencyHistogram struct {
	s *histSeries
}

func (t *latencyHistogram) Timer() Timer {
	return Timer{t, time.Now()}
}

func NewLatencyHistogram(name string, description string, labels map[string]any) LatencyHistogram {
	// The sum is kept in microseconds, and exported in milliseconds.
	return &latencyHistogram{
		s: getHistogram(histID{name, description, Milliseconds}, latencyBucketsMillis, 1000).series(labels),
	}
}

// Histograms are kept in adders and exported by HistogramProducer: a
// synchronous OTel histogram looks up the attribute set, takes a lock and
// allocates on every record. The exported data is the same as what the SDK
// aggregates with explicit buckets, as long as the producer keeps its
// semantics:
//   - histograms with the same identity and labels record into one series,
//   - a series is reported from its first record on, and never dropped,
//   - a value goes into the first bucket whose bound is >= the value.

// histID is what the SDK identifies a histogram by.
type histID struct {
	name        string
	description string
	unit        Unit
}

type histSeries struct {
	bounds []float64
	// buckets[i] counts the values in (bounds[i-1], bounds[i]]; the last one
	// the values above all bounds.
	buckets  []adder
	sum      adder
	recorded atomic.Bool
	attrs    attribute.Set
}

func (s *histSeries) record(value float64, sumValue int64) {
	s.buckets[sort.SearchFloat64s(s.bounds, value)].Add(1)
	s.sum.Add(sumValue)
	if !s.recorded.Load() {
		s.recorded.Store(true)
	}
}

type observedHistogram struct {
	sync.Mutex
	id     histID
	bounds []float64
	// sumDivisor converts the recorded sum into the exported unit. When it
	// is not 1, the histogram is exported with float64 values.
	sumDivisor int64
	start      time.Time
	byAttrs    map[attribute.Distinct]*histSeries
}

func (o *observedHistogram) series(labels map[string]any) *histSeries {
	set := getAttrSet(labels)
	o.Lock()
	defer o.Unlock()
	s, ok := o.byAttrs[set.Equivalent()]
	if !ok {
		s = &histSeries{
			bounds:  o.bounds,
			buckets: make([]adder, len(o.bounds)+1),
			attrs:   set,
		}
		o.byAttrs[set.Equivalent()] = s
	}
	return s
}

func histogramDataPoint[N int64 | float64](o *observedHistogram, s *histSeries, now time.Time) metricdata.HistogramDataPoint[N] {
	counts := make([]uint64, len(s.buckets))
	var count uint64
	for i := range s.buckets {
		counts[i] = uint64(s.buckets[i].Sum())
		count += counts[i]
	}
	var sum N
	if o.sumDivisor == 1 {
		sum = N(s.sum.Sum())
	} else {
		sum = N(float64(s.sum.Sum()) / float64(o.sumDivisor))
	}
	return metricdata.HistogramDataPoint[N]{
		Attributes:   s.attrs,
		StartTime:    o.start,
		Time:         now,
		Count:        count,
		Bounds:       o.bounds,
		BucketCounts: counts,
		Sum:          sum,
	}
}

func histogramData[N int64 | float64](o *observedHistogram, now time.Time) (metricdata.Histogram[N], bool) {
	o.Lock()
	defer o.Unlock()
	h := metricdata.Histogram[N]{Temporality: metricdata.CumulativeTemporality}
	for _, s := range o.byAttrs {
		if s.recorded.Load() {
			h.DataPoints = append(h.DataPoints, histogramDataPoint[N](o, s, now))
		}
	}
	return h, len(h.DataPoints) > 0
}

func (o *observedHistogram) metrics(now time.Time) (metricdata.Metrics, bool) {
	m := metricdata.Metrics{Name: o.id.name, Description: o.id.description, Unit: string(o.id.unit)}
	var ok bool
	if o.sumDivisor == 1 {
		m.Data, ok = histogramData[int64](o, now)
	} else {
		m.Data, ok = histogramData[float64](o, now)
	}
	return m, ok
}

var (
	histogramsLock sync.Mutex
	histograms     = map[histID]*observedHistogram{}
)

func getHistogram(id histID, bounds []float64, sumDivisor int64) *observedHistogram {
	histogramsLock.Lock()
	defer histogramsLock.Unlock()
	o, ok := histograms[id]
	if !ok {
		o = &observedHistogram{
			id:         id,
			bounds:     bounds,
			sumDivisor: sumDivisor,
			start:      time.Now(),
			byAttrs:    map[attribute.Distinct]*histSeries{},
		}
		histograms[id] = o
	}
	return o
}

type histogramProducer struct{}

// HistogramProducer exports the histograms. It must be registered on the
// metric reader, e.g. with prometheus.WithProducer.
var HistogramProducer histogramProducer

func (histogramProducer) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	histogramsLock.Lock()
	all := make([]*observedHistogram, 0, len(histograms))
	for _, o := range histograms {
		all = append(all, o)
	}
	histogramsLock.Unlock()

	now := time.Now()
	sm := metricdata.ScopeMetrics{Scope: instrumentation.Scope{Name: meterName}}
	for _, o := range all {
		if m, ok := o.metrics(now); ok {
			sm.Metrics = append(sm.Metrics, m)
		}
	}
	if len(sm.Metrics) == 0 {
		return nil, nil
	}
	return []metricdata.ScopeMetrics{sm}, nil
}
