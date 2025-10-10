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

package concurrent

import (
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func WaitUntilSignal(closers ...io.Closer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	sig := <-c
	slog.Info(
		"Received signal, exiting",
		slog.String("signal", sig.String()),
	)

	code := 0
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			slog.Error(
				"Failed when shutting down server",
				slog.Any("error", err),
			)
			code = 1
		}
	}

	if code == 0 {
		slog.Info("Shutdown Completed")
	}
	os.Exit(code)
}
