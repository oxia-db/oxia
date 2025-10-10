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
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/oxia-db/oxia/common/process"
)

func init() {
	exporter, err := prometheus.New()
	if err != nil {
		slog.Error(
			"Failed to initialize Prometheus metrics exporter",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	// Use a specific list of buckets for different types of histograms
	latencyHistogramView := metric.NewView(
		metric.Instrument{
			Kind: metric.InstrumentKindHistogram,
			Unit: string(Milliseconds),
		},
		metric.Stream{
			Aggregation: metric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBucketsMillis,
			},
		},
	)
	sizeHistogramView := metric.NewView(
		metric.Instrument{
			Kind: metric.InstrumentKindHistogram,
			Unit: string(Bytes),
		},
		metric.Stream{
			Aggregation: metric.AggregationExplicitBucketHistogram{
				Boundaries: sizeBucketsBytes,
			},
		},
	)
	countHistogramView := metric.NewView(
		metric.Instrument{
			Kind: metric.InstrumentKindHistogram,
			Unit: string(Dimensionless),
		},
		metric.Stream{
			Aggregation: metric.AggregationExplicitBucketHistogram{
				Boundaries: sizeBucketsCount,
			},
		},
	)

	// Default view to keep all instruments
	defaultView := metric.NewView(metric.Instrument{Name: "*"}, metric.Stream{})

	provider := metric.NewMeterProvider(metric.WithReader(exporter),
		metric.WithView(latencyHistogramView, sizeHistogramView, countHistogramView, defaultView))
	meter = provider.Meter("oxia")
}

type PrometheusMetrics struct {
	io.Closer

	server *http.Server
	port   int
}

func Start(bindAddress string) (*PrometheusMetrics, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	p := &PrometheusMetrics{
		server: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: time.Second,
		},
		port: listener.Addr().(*net.TCPAddr).Port,
	}

	slog.Info(fmt.Sprintf("Serving Prometheus metrics at http://localhost:%d/metrics", p.port))

	go process.DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia": "metrics",
		},
		func() {
			if err = p.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error(
					"Failed to serve metrics",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
		},
	)

	return p, nil
}

func (p *PrometheusMetrics) Port() int {
	return p.port
}

func (p *PrometheusMetrics) Close() error {
	return p.server.Close()
}
