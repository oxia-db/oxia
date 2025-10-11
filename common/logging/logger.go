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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
)

const DefaultLogLevel = slog.LevelInfo

var (
	// LogLevel Used for flags.
	LogLevel slog.Level
	// LogJSON Used for flags.
	LogJSON bool
)

// ParseLogLevel will convert the slog level configuration to slog.Level values.
func ParseLogLevel(levelStr string) (slog.Level, error) {
	switch {
	case strings.EqualFold(levelStr, slog.LevelDebug.String()):
		return slog.LevelDebug, nil
	case strings.EqualFold(levelStr, slog.LevelInfo.String()):
		return slog.LevelInfo, nil
	case strings.EqualFold(levelStr, slog.LevelWarn.String()):
		return slog.LevelWarn, nil
	case strings.EqualFold(levelStr, slog.LevelError.String()):
		return slog.LevelError, nil
	}

	return slog.LevelInfo, fmt.Errorf("unknown level string: '%s', defaulting to LevelInfo", levelStr)
}

func ConfigureLogger() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	//nolint
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	protoMarshal := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	zerolog.InterfaceMarshalFunc = func(i any) ([]byte, error) {
		if m, ok := i.(pb.Message); ok {
			return protoMarshal.Marshal(m)
		}
		return json.Marshal(i)
	}

	zerologLogger := zerolog.New(os.Stdout).
		With().
		Timestamp().
		Stack().
		Logger()

	if !LogJSON {
		zerologLogger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.StampMicro,
		})
	}

	slogLogger := slog.New(
		slogzerolog.Option{
			Level:  LogLevel,
			Logger: &zerologLogger,
		}.NewZerologHandler(),
	)
	slog.SetDefault(slogLogger)
}
