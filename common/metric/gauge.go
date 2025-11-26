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
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/metric"
)

type Gauge interface {
	Unregister()
}

type gauge struct {
	gauge        metric.Int64ObservableGauge
	attrs        metric.MeasurementOption
	callback     func() int64
	registration metric.Registration
}

func (g *gauge) Unregister() {
	if err := g.registration.Unregister(); err != nil {
		slog.Error(
			"Failed to unregister gauge",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
}

func NewGauge(name string, description string, unit Unit, labels map[string]any, callback func() int64) Gauge {
	g, err := GetMeter().Int64ObservableGauge(name,
		metric.WithUnit(string(unit)),
		metric.WithDescription(description),
	)
	fatalOnErr(err, name)

	res := &gauge{
		gauge:    g,
		attrs:    getAttrs(labels),
		callback: callback,
	}

	res.registration, err = meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		obs.ObserveInt64(res.gauge, res.callback(), res.attrs)
		return nil
	}, g)

	if err != nil {
		slog.Error(
			"Failed to register gauge",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	return res
}
