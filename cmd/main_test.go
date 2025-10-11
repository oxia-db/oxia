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

package main

import (
	"log/slog"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/logging"
)

func TestCall_LogLevel_Default(t *testing.T) {
	var captured slog.Level
	rootCmd.SetArgs([]string{})
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		captured = logging.LogLevel
		return nil
	}
	err := rootCmd.Execute()
	assert.Equal(t, logging.DefaultLogLevel, captured)
	assert.NoError(t, err)
}

func TestCall_LogLevel(t *testing.T) {
	tests := []struct {
		name          string
		level         string
		expectedErr   error
		expectedLevel slog.Level
	}{
		{"debug", "debug", nil, slog.LevelDebug},
		{"info", "info", nil, slog.LevelInfo},
		{"warn", "warn", nil, slog.LevelWarn},
		{"error", "error", nil, slog.LevelError},
		{"junk", "junk", LogLevelError("junk"), slog.LevelInfo},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invoked := false
			rootCmd.SetArgs(append([]string{"-l"}, test.level))
			rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
				invoked = true
				assert.Equal(t, test.expectedLevel, logging.LogLevel)
				return nil
			}
			err := rootCmd.Execute()
			if err == nil {
				assert.True(t, invoked)
			} else {
				assert.False(t, invoked)
			}
			assert.ErrorIs(t, err, test.expectedErr)
		})
	}
}
