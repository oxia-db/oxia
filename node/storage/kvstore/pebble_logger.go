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

package kvstore

import (
	"fmt"
	"log/slog"
	"os"
)

// pebbleLogger is the wrapper of slog to implement pebbel's logger interface.
type pebbleLogger struct {
	zl *slog.Logger
}

func (pl *pebbleLogger) Infof(format string, args ...any) {
	pl.zl.Info(
		fmt.Sprintf(format, args...),
	)
}

func (pl *pebbleLogger) Errorf(format string, args ...any) {
	pl.zl.Error(
		fmt.Sprintf(format, args...),
	)
}

func (pl *pebbleLogger) Fatalf(format string, args ...any) {
	pl.zl.Warn(
		fmt.Sprintf(format, args...),
	)
	os.Exit(1)
}
