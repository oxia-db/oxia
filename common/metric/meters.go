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
	"fmt"
	"log/slog"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter metric.Meter
	once  sync.Once
)

func GetMeter() metric.Meter {
	once.Do(func() {
		meter = otel.GetMeterProvider().Meter(
			"oxia",
		)
	})
	return meter
}

func SetMeter(m metric.Meter) {
	meter = m
}

func LabelsForShard(namespace string, shard int64) map[string]any {
	return map[string]any{
		"shard":          shard,
		"oxia_namespace": namespace,
	}
}

func fatalOnErr(err error, name string) {
	if err != nil {
		slog.Error(
			"Failed to create metric",
			slog.String("metric-name", name),
		)
		os.Exit(1)
	}
}

func getAttrs(labels map[string]any) (options metric.MeasurementOption) {
	attrs := make([]attribute.KeyValue, 0)
	for k, v := range labels {
		key := attribute.Key(k)
		var attr attribute.KeyValue
		switch t := v.(type) {
		case uint32:
			attr = key.Int64(int64(t))
		case int64:
			attr = key.Int64(t)
		case int:
			attr = key.Int(t)
		case float64:
			attr = key.Float64(t)
		case bool:
			attr = key.Bool(t)
		case string:
			attr = key.String(t)

		default:
			slog.Error(fmt.Sprintf("Invalid label type %#v", v))
			os.Exit(1)
		}

		attrs = append(attrs, attr)
	}

	return metric.WithAttributes(attrs...)
}
