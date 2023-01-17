// Copyright 2023 StreamNative, Inc.
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"time"
)

type Timer interface {
	Record(ctx context.Context, incr time.Duration, attrs ...attribute.KeyValue)
}

type timerImpl struct {
	sum   syncfloat64.Counter
	count syncint64.Counter
}

func newTimer(meter metric.Meter, name string) Timer {
	return &timerImpl{
		sum:   newMillisCounter(meter, name),
		count: newCounter(meter, name, ""),
	}
}

func (t *timerImpl) Record(ctx context.Context, incr time.Duration, attrs ...attribute.KeyValue) {
	millis := float64(incr) / float64(time.Millisecond)
	t.sum.Add(ctx, millis, attrs...)
	t.count.Add(ctx, 1, attrs...)
}
