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

package truncate

import (
	"log/slog"
	"math"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/server/wal"
)

type truncateOptions struct {
	lastEntry      bool
	safePointEntry int64
}

var (
	options = truncateOptions{}
	Cmd     = &cobra.Command{
		Use:   "truncate",
		Short: "truncate the WAL",
		Long:  `truncate the WAL by some conditions`,
		RunE:  exec,
	}
)

func init() {
	// operations
	Cmd.Flags().Int64Var(&options.safePointEntry, "last-safe-entry", math.MaxInt64,
		"removes entries from the end of the log that have an offset greater than last safe entry")
	Cmd.Flags().BoolVar(&options.lastEntry, "truncate-last-entry", false, "removes the last entry of the log")

	Cmd.MarkFlagsMutuallyExclusive("last-safe-entry", "truncate-last-entry")
}

func exec(*cobra.Command, []string) error {
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
		SyncData:    true,
	})
	writeAheadLog, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	if err != nil {
		panic(err)
	}
	defer writeAheadLog.Close()

	if options.safePointEntry != math.MaxInt64 {
		slog.Info("truncating the entries. ", slog.Int64("start", options.safePointEntry),
			slog.Int64("end", writeAheadLog.LastOffset()))
		newLastEntry, err := writeAheadLog.TruncateLog(options.safePointEntry)
		if err != nil {
			return err
		}
		slog.Info("truncate complete", slog.Int64("newLastEntry", newLastEntry))
		return nil
	}

	if options.lastEntry {
		lastOffset := writeAheadLog.LastOffset()
		safePoint := lastOffset - 1
		slog.Info("truncating the last entry. ",
			slog.Int64("start", safePoint),
			slog.Int64("end", lastOffset))
		newLastEntry, err := writeAheadLog.TruncateLog(safePoint)
		if err != nil {
			return err
		}
		slog.Info("truncate complete", slog.Int64("newLastEntry", newLastEntry))
		return nil
	}

	slog.Info("no operation applied")
	return nil
}
