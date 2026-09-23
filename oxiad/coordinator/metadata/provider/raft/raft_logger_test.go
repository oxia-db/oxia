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

package raft

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

func newTestHclogAdapter(handlerLevel slog.Leveler) (hclog.Logger, *slog.LevelVar, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: handlerLevel,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	level := &slog.LevelVar{}
	logger := slog.New(handler).With(slog.String("component", "test"))
	return newHclogAdapter(logger, level), level, buf
}

func TestHclogAdapterLevels(t *testing.T) {
	for _, test := range []struct {
		name     string
		log      func(logger hclog.Logger, msg string, args ...any)
		expected string
	}{
		{"trace", hclog.Logger.Trace, "DEBUG-4"},
		{"debug", hclog.Logger.Debug, "DEBUG"},
		{"info", hclog.Logger.Info, "INFO"},
		{"warn", hclog.Logger.Warn, "WARN"},
		{"error", hclog.Logger.Error, "ERROR"},
		{"log-trace", logAt(hclog.Trace), "DEBUG-4"},
		{"log-debug", logAt(hclog.Debug), "DEBUG"},
		{"log-info", logAt(hclog.Info), "INFO"},
		{"log-warn", logAt(hclog.Warn), "WARN"},
		{"log-error", logAt(hclog.Error), "ERROR"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logger, _, buf := newTestHclogAdapter(slogLevelTrace)

			test.log(logger, "entering follower state", "leader-id", "n1", "term", 3)

			assert.Equal(t,
				"level="+test.expected+" msg=\"entering follower state\" component=test leader-id=n1 term=3\n",
				buf.String())
		})
	}
}

func logAt(level hclog.Level) func(logger hclog.Logger, msg string, args ...any) {
	return func(logger hclog.Logger, msg string, args ...any) {
		logger.Log(level, msg, args...)
	}
}

// The slog handler is the one deciding what gets emitted, following its level
// even when it is reconfigured at runtime. The level variable has no say.
func TestHclogAdapterHandlerDecidesWhatIsEmitted(t *testing.T) {
	handlerLevel := &slog.LevelVar{}
	logger, level, buf := newTestHclogAdapter(handlerLevel)
	assert.Equal(t, slog.LevelInfo, level.Level())

	logger.Debug("dropped")
	assert.Empty(t, buf.String())
	assert.False(t, logger.IsTrace())
	assert.False(t, logger.IsDebug())
	assert.True(t, logger.IsInfo())
	assert.True(t, logger.IsWarn())
	assert.True(t, logger.IsError())

	handlerLevel.Set(slog.LevelDebug)

	logger.Debug("emitted")
	assert.Equal(t, "level=DEBUG msg=emitted component=test\n", buf.String())
	assert.False(t, logger.IsTrace())
	assert.True(t, logger.IsDebug())

	buf.Reset()
	handlerLevel.Set(slog.LevelError)

	logger.Warn("dropped")
	assert.Empty(t, buf.String())
	assert.False(t, logger.IsWarn())
	assert.True(t, logger.IsError())
}

func TestHclogAdapterWith(t *testing.T) {
	logger, _, buf := newTestHclogAdapter(slog.LevelInfo)

	snapshot := logger.With("id", "2-10-1", "last-index", 10)
	snapshot.Info("snapshot restore progress")
	assert.Equal(t, "level=INFO msg=\"snapshot restore progress\" component=test id=2-10-1 last-index=10\n",
		buf.String())

	buf.Reset()
	snapshot.With("size", 42).Warn("nested", "percent-complete", 50)
	assert.Equal(t, "level=WARN msg=nested component=test id=2-10-1 last-index=10 size=42 percent-complete=50\n",
		buf.String())

	// The parent logger is left untouched
	buf.Reset()
	logger.Info("parent")
	assert.Equal(t, "level=INFO msg=parent component=test\n", buf.String())
}

// Raft does not call these: they only have to keep the logs flowing should
// that ever change.
func TestHclogAdapterUnusedMethods(t *testing.T) {
	logger, _, buf := newTestHclogAdapter(slog.LevelInfo)

	named := logger.Named("snapshot").ResetNamed("fsm")
	assert.Empty(t, named.Name())
	assert.Nil(t, named.ImpliedArgs())
	named.Info("named")
	assert.Equal(t, "level=INFO msg=named component=test\n", buf.String())

	buf.Reset()
	logger.StandardLogger(nil).Print("from the standard logger")
	assert.Equal(t, "level=INFO msg=\"from the standard logger\" component=test\n", buf.String())

	buf.Reset()
	_, err := logger.StandardWriter(nil).Write([]byte("from the standard writer\n"))
	assert.NoError(t, err)
	assert.Equal(t, "level=INFO msg=\"from the standard writer\" component=test\n", buf.String())
}

func TestHclogAdapterLevelVariable(t *testing.T) {
	logger, level, _ := newTestHclogAdapter(slog.LevelInfo)

	for _, test := range []struct {
		hclogLevel hclog.Level
		slogLevel  slog.Level
	}{
		{hclog.Trace, slogLevelTrace},
		{hclog.Debug, slog.LevelDebug},
		{hclog.Info, slog.LevelInfo},
		{hclog.Warn, slog.LevelWarn},
		{hclog.Error, slog.LevelError},
		{hclog.Off, slog.LevelError + 4},
	} {
		t.Run(test.hclogLevel.String(), func(t *testing.T) {
			level.Set(test.slogLevel)
			assert.Equal(t, test.hclogLevel, logger.GetLevel())

			level.Set(slog.LevelInfo)
			logger.SetLevel(test.hclogLevel)
			assert.Equal(t, test.slogLevel, level.Level())
		})
	}
}
