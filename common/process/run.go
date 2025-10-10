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
	"io"
	"log/slog"
	"os"

	"github.com/oxia-db/oxia/common/concurrent"
)

func RunProcess(startProcess func() (io.Closer, error)) {
	profiler := RunProfiling()
	process, err := startProcess()
	if err != nil {
		slog.Error(
			"Failed to start the process",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	concurrent.WaitUntilSignal(
		process,
		profiler,
	)
}
