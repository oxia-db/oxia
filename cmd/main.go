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
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/oxia-db/oxia/cmd/config"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"

	"github.com/oxia-db/oxia/cmd/admin"
	"github.com/oxia-db/oxia/cmd/client"
	"github.com/oxia-db/oxia/cmd/coordinator"
	"github.com/oxia-db/oxia/cmd/health"
	"github.com/oxia-db/oxia/cmd/pebble"
	"github.com/oxia-db/oxia/cmd/perf"
	"github.com/oxia-db/oxia/cmd/server"
	"github.com/oxia-db/oxia/cmd/standalone"
	"github.com/oxia-db/oxia/cmd/wal"
)

var (
	version     string
	logLevelStr string
	rootCmd     = &cobra.Command{
		Use:               "oxia",
		Short:             "Oxia root command",
		Long:              `Oxia root command`,
		PersistentPreRunE: configureLogLevel,
		SilenceUsage:      true,
		Version:           version,
	}
)

type LogLevelError string

func (l LogLevelError) Error() string {
	return fmt.Sprintf("unknown log level (%s)", string(l))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", logging.DefaultLogLevel.String(), "Set logging level [debug|info|warn|error]")
	rootCmd.PersistentFlags().BoolVarP(&logging.LogJSON, "log-json", "j", false, "Print logs in JSON format")
	rootCmd.PersistentFlags().BoolVar(&process.PprofEnable, "profile", false, "Enable pprof profiler")
	rootCmd.PersistentFlags().StringVar(&process.PprofBindAddress, "profile-bind-address", "127.0.0.1:6060", "Bind address for pprof")

	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(coordinator.Cmd)
	rootCmd.AddCommand(health.Cmd)
	rootCmd.AddCommand(perf.Cmd)
	rootCmd.AddCommand(server.Cmd)
	rootCmd.AddCommand(standalone.Cmd)
	rootCmd.AddCommand(pebble.Cmd)
	rootCmd.AddCommand(wal.Cmd)
	rootCmd.AddCommand(admin.Cmd)
	rootCmd.AddCommand(config.Cmd)
}

func configureLogLevel(_ *cobra.Command, _ []string) error {
	logLevel, err := logging.ParseLogLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	logging.LogLevel = logLevel
	logging.ConfigureLogger()
	return nil
}

func main() {
	process.DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia": "main",
		},
		func() {
			if _, err := maxprocs.Set(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := rootCmd.Execute(); err != nil {
				os.Exit(1)
			}
		},
	)
}
