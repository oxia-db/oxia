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

package process

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"os"
	"runtime/pprof"
	"time"
)

var (
	PprofEnable      bool
	PprofBindAddress string
)

// DoWithLabels attaches the labels to the current go-routine Pprof context,
// for the duration of the call to f.
func DoWithLabels(ctx context.Context, labels map[string]string, f func()) {
	l := make([]string, 0, len(labels)*2)
	for k, v := range labels {
		l = append(l, k, v)
	}

	pprof.Do(
		ctx,
		pprof.Labels(l...),
		func(_ context.Context) {
			f()
		})
}

func RunProfiling() io.Closer {
	s := &http.Server{
		Addr:              PprofBindAddress,
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: time.Second,
	}

	if !PprofEnable {
		// Do not start pprof server
		return s
	}

	slog.Info(
		"Starting pprof server",
		slog.String("address", s.Addr),
	)
	slog.Info(fmt.Sprintf("  use http://%s/debug/pprof to access the browser", s.Addr))
	slog.Info(fmt.Sprintf("  use `go tool pprof http://%s/debug/pprof/profile` to get pprof file(cpu info)", s.Addr))
	slog.Info(fmt.Sprintf("  use `go tool pprof http://%s/debug/pprof/heap` to get inuse_space file", s.Addr))
	slog.Info("")

	go DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia": "pprof",
		},
		func() {
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error(
					"Unable to start debug profiling server",
					slog.Any("error", err),
					slog.String("component", "pprof"),
				)
				os.Exit(1)
			}
		})

	return s
}
