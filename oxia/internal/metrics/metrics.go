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
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	ometric "github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

// Metrics records the client metrics of the operations and of the batches.
//
// It runs on the path of every operation, so it builds the attributes of each
// request type once, and it decorates nothing when the provider is the no-op
// one, which is the default of the client.
type Metrics struct {
	enabled   bool
	timeFunc  func() time.Time
	sinceFunc func(time.Time) time.Duration

	opTime  Timer
	opValue metric.Int64Histogram

	batchTotalTime Timer
	batchExecTime  Timer
	batchValue     metric.Int64Histogram
	batchRequests  metric.Int64Histogram

	putAttrs         requestAttrs
	deleteAttrs      requestAttrs
	deleteRangeAttrs requestAttrs
	getAttrs         requestAttrs
	writeAttrs       requestAttrs
	readAttrs        requestAttrs
}

func NewMetrics(provider metric.MeterProvider) *Metrics {
	return newMetrics(provider, time.Now, time.Since)
}

func newMetrics(provider metric.MeterProvider, timeFunc func() time.Time, sinceFunc func(time.Time) time.Duration) *Metrics {
	if _, isNoop := provider.(noop.MeterProvider); isNoop {
		return &Metrics{}
	}

	meter := provider.Meter("oxia_client")
	return &Metrics{
		enabled:   true,
		timeFunc:  timeFunc,
		sinceFunc: sinceFunc,

		opTime:  newTimer(meter, "oxia_client_op"),
		opValue: newHistogram(meter, "oxia_client_op_value", ometric.Bytes),

		batchTotalTime: newTimer(meter, "oxia_client_batch_total"),
		batchExecTime:  newTimer(meter, "oxia_client_batch_exec"),
		batchValue:     newHistogram(meter, "oxia_client_batch_value", ometric.Bytes),
		batchRequests:  newHistogram(meter, "oxia_client_batch_request", ""),

		putAttrs:         newRequestAttrs("put"),
		deleteAttrs:      newRequestAttrs("delete"),
		deleteRangeAttrs: newRequestAttrs("delete_range"),
		getAttrs:         newRequestAttrs("get"),
		writeAttrs:       newRequestAttrs("write"),
		readAttrs:        newRequestAttrs("read"),
	}
}

func (m *Metrics) DecoratePut(put model.PutCall) model.PutCall {
	if !m.enabled {
		return put
	}
	callback := put.Callback
	start := m.timeFunc()
	// Capture the size, not put: a closure that captures put moves it to the
	// heap on every call, even when metrics are off.
	valueSize := int64(len(put.Value))
	put.Callback = func(response *proto.PutResponse, err error) {
		callback(response, err)
		attrs := m.putAttrs.of(err)
		m.opTime.Record(context.TODO(), m.sinceFunc(start), attrs)
		m.opValue.Record(context.TODO(), valueSize, attrs)
	}
	return put
}

func (m *Metrics) DecorateDelete(deleteCall model.DeleteCall) model.DeleteCall {
	if !m.enabled {
		return deleteCall
	}
	callback := deleteCall.Callback
	start := m.timeFunc()
	deleteCall.Callback = func(response *proto.DeleteResponse, err error) {
		callback(response, err)
		m.opTime.Record(context.TODO(), m.sinceFunc(start), m.deleteAttrs.of(err))
	}
	return deleteCall
}

func (m *Metrics) DecorateDeleteRange(deleteRange model.DeleteRangeCall) model.DeleteRangeCall {
	if !m.enabled {
		return deleteRange
	}
	callback := deleteRange.Callback
	start := m.timeFunc()
	deleteRange.Callback = func(response *proto.DeleteRangeResponse, err error) {
		callback(response, err)
		m.opTime.Record(context.TODO(), m.sinceFunc(start), m.deleteRangeAttrs.of(err))
	}
	return deleteRange
}

func (m *Metrics) DecorateGet(get model.GetCall) model.GetCall {
	if !m.enabled {
		return get
	}
	callback := get.Callback
	start := m.timeFunc()
	get.Callback = func(response *proto.GetResponse, err error) {
		callback(response, err)
		attrs := m.getAttrs.of(err)
		m.opTime.Record(context.TODO(), m.sinceFunc(start), attrs)
		var size int64
		if response != nil {
			size = int64(len(response.Value))
		}
		m.opValue.Record(context.TODO(), size, attrs)
	}
	return get
}

func (m *Metrics) WriteCallback() func(time.Time, *proto.WriteRequest, *proto.WriteResponse, error) {
	if !m.enabled {
		return func(time.Time, *proto.WriteRequest, *proto.WriteResponse, error) {}
	}
	batchStart := m.timeFunc()
	return func(executionStart time.Time, request *proto.WriteRequest, _ *proto.WriteResponse, err error) {
		attrs := m.writeAttrs.of(err)
		m.batchTotalTime.Record(context.TODO(), m.sinceFunc(batchStart), attrs)
		m.batchExecTime.Record(context.TODO(), m.sinceFunc(executionStart), attrs)
		valueSize, requestCount := writeMetrics(request)
		m.batchValue.Record(context.TODO(), valueSize, attrs)
		m.batchRequests.Record(context.TODO(), requestCount, attrs)
	}
}

func (m *Metrics) ReadCallback() func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error) {
	if !m.enabled {
		return func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error) {}
	}
	batchStart := m.timeFunc()
	return func(executionStart time.Time, _ *proto.ReadRequest, response *proto.ReadResponse, err error) {
		attrs := m.readAttrs.of(err)
		m.batchTotalTime.Record(context.TODO(), m.sinceFunc(batchStart), attrs)
		m.batchExecTime.Record(context.TODO(), m.sinceFunc(executionStart), attrs)
		valueSize, requestCount := readMetrics(response)
		m.batchValue.Record(context.TODO(), valueSize, attrs)
		m.batchRequests.Record(context.TODO(), requestCount, attrs)
	}
}

// requestAttrs holds the attributes of a request type for each result.
type requestAttrs struct {
	success metric.MeasurementOption
	failure metric.MeasurementOption
}

func newRequestAttrs(requestType string) requestAttrs {
	return requestAttrs{
		success: attrs(requestType, resultSuccess),
		failure: attrs(requestType, resultFailure),
	}
}

func (a requestAttrs) of(err error) metric.MeasurementOption {
	if err == nil {
		return a.success
	}
	return a.failure
}
