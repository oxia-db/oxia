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
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func TestPrometheusMetrics(t *testing.T) {
	metrics, err := Start("localhost:0", nil)
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))

	err = metrics.Close()
	assert.NoError(t, err)

	response2, err := http.Get(url)
	assert.ErrorContains(t, err, "connection refused")
	assert.Nil(t, response2)

	if response2 != nil && response2.Body != nil {
		defer response2.Body.Close()
	}
}

// The OTel SDK defaults to 2000 attribute sets per instrument, collapsing the
// rest into an `otel.metric.overflow` series. Per-shard metrics can exceed that.
func TestNoCardinalityLimit(t *testing.T) {
	const name = "oxia_test_cardinality"
	const attributeSets = 2500

	counter, err := otel.Meter("oxia-test").Int64Counter(name)
	assert.NoError(t, err)

	for i := 0; i < attributeSets; i++ {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.Int("shard", i)))
	}

	families, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	series := 0
	for _, family := range families {
		if family.GetName() != name+"_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			series++
			for _, label := range m.GetLabel() {
				assert.NotEqual(t, "otel_metric_overflow", label.GetName())
			}
		}
	}
	assert.Equal(t, attributeSets, series)
}
