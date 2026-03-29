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

package logging

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/common/option"
)

func TestReconfigureLogger_DynamicLevelChange(t *testing.T) {
	// Start with INFO level
	LogLevel = slog.LevelInfo
	LogJSON = true
	ConfigureLogger()

	// Create a derived logger before the level change, simulating
	// component loggers created at startup via slog.With().
	derivedLogger := slog.Default().With("component", "test")

	// The derived logger should not enable DEBUG at INFO level
	assert.False(t, derivedLogger.Enabled(context.Background(), slog.LevelDebug))
	assert.True(t, derivedLogger.Enabled(context.Background(), slog.LevelInfo))

	// Reconfigure to DEBUG level
	changed := ReconfigureLogger(&option.LogOptions{Level: "debug"})
	assert.True(t, changed)

	// The derived logger (created before reconfigure) should now enable DEBUG
	assert.True(t, derivedLogger.Enabled(context.Background(), slog.LevelDebug))
}

func TestReconfigureLogger_NoChangeReturnsFalse(t *testing.T) {
	LogLevel = slog.LevelInfo
	LogJSON = true
	ConfigureLogger()

	changed := ReconfigureLogger(&option.LogOptions{Level: "info"})
	assert.False(t, changed)
}
