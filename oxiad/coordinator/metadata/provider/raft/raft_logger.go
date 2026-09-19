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
	"context"
	"io"
	"log"
	"log/slog"

	"github.com/hashicorp/go-hclog"
)

// slogLevelTrace stands for hclog.Trace, which has no slog counterpart.
const slogLevelTrace = slog.LevelDebug - 4

// hclogAdapter exposes a *slog.Logger as the hclog.Logger that hashicorp/raft
// logs to. Raft only calls Debug, Info, Warn, Error and With: the rest of the
// interface is implemented just enough to satisfy it, and logger names are
// not supported.
//
// The slog handler decides what gets emitted: level only backs GetLevel and
// SetLevel.
type hclogAdapter struct {
	logger *slog.Logger
	level  *slog.LevelVar
}

var _ hclog.Logger = (*hclogAdapter)(nil)

func newHclogAdapter(logger *slog.Logger, level *slog.LevelVar) hclog.Logger {
	return &hclogAdapter{logger: logger, level: level}
}

// Both scales are linear: hclog goes from Trace=1 to Off=6 in steps of 1,
// slog in steps of 4 around Info=0.
func toSlogLevel(level hclog.Level) slog.Level {
	return slog.Level(level-hclog.Info) * 4
}

func toHclogLevel(level slog.Level) hclog.Level {
	return hclog.Level(level/4) + hclog.Info
}

func (a *hclogAdapter) Log(level hclog.Level, msg string, args ...any) {
	a.logger.Log(context.Background(), toSlogLevel(level), msg, args...)
}

func (a *hclogAdapter) Trace(msg string, args ...any) {
	a.logger.Log(context.Background(), slogLevelTrace, msg, args...)
}

func (a *hclogAdapter) Debug(msg string, args ...any) {
	a.logger.Debug(msg, args...)
}

func (a *hclogAdapter) Info(msg string, args ...any) {
	a.logger.Info(msg, args...)
}

func (a *hclogAdapter) Warn(msg string, args ...any) {
	a.logger.Warn(msg, args...)
}

func (a *hclogAdapter) Error(msg string, args ...any) {
	a.logger.Error(msg, args...)
}

func (a *hclogAdapter) IsTrace() bool {
	return a.logger.Enabled(context.Background(), slogLevelTrace)
}

func (a *hclogAdapter) IsDebug() bool {
	return a.logger.Enabled(context.Background(), slog.LevelDebug)
}

func (a *hclogAdapter) IsInfo() bool {
	return a.logger.Enabled(context.Background(), slog.LevelInfo)
}

func (a *hclogAdapter) IsWarn() bool {
	return a.logger.Enabled(context.Background(), slog.LevelWarn)
}

func (a *hclogAdapter) IsError() bool {
	return a.logger.Enabled(context.Background(), slog.LevelError)
}

func (*hclogAdapter) ImpliedArgs() []any {
	return nil
}

func (a *hclogAdapter) With(args ...any) hclog.Logger {
	return &hclogAdapter{logger: a.logger.With(args...), level: a.level}
}

func (*hclogAdapter) Name() string {
	return ""
}

func (a *hclogAdapter) Named(string) hclog.Logger {
	return a
}

func (a *hclogAdapter) ResetNamed(string) hclog.Logger {
	return a
}

func (a *hclogAdapter) SetLevel(level hclog.Level) {
	a.level.Set(toSlogLevel(level))
}

func (a *hclogAdapter) GetLevel() hclog.Level {
	return toHclogLevel(a.level.Level())
}

func (a *hclogAdapter) StandardLogger(*hclog.StandardLoggerOptions) *log.Logger {
	return slog.NewLogLogger(a.logger.Handler(), slog.LevelInfo)
}

func (a *hclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return a.StandardLogger(opts).Writer()
}
