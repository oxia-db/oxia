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

package scan

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/oxia-db/oxia/proto"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/dataserver/wal"
)

type scanOptions struct {
	json        bool
	firstOffset int64
	lastOffset  int64
}

var (
	options = scanOptions{}
	Cmd     = &cobra.Command{
		Use:   "scan",
		Short: "scan the WAL",
		Long:  `scan the WAL`,
		RunE:  exec,
	}
)

func init() {
	// operations
	Cmd.Flags().Int64Var(&options.firstOffset, "offset-first", -1,
		"offset where to start reading from")
	Cmd.Flags().Int64Var(&options.lastOffset, "offset-last", -1,
		"last offset to read")
	Cmd.Flags().BoolVar(&options.json, "json", false, "dump entries in json format")
}

type LogEntry struct {
	Term          int64                `json:"term"`
	Offset        int64                `json:"offset"`
	Timestamp     time.Time            `json:"time"`
	WriteRequests *proto.WriteRequests `json:"writeRequests"`
}

func exec(*cobra.Command, []string) error {
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
		SyncData:    false,
	})

	_wal, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	if err != nil {
		return err
	}
	defer func(_wal wal.Wal) {
		_ = _wal.Close()
	}(_wal)

	firstOffset := options.firstOffset
	if firstOffset < 0 {
		firstOffset = _wal.FirstOffset() - 1
	}

	reader, err := _wal.NewReader(firstOffset)
	if err != nil {
		return err
	}

	for reader.HasNext() {
		le, err := reader.ReadNext()
		if err != nil {
			return err
		}

		if options.lastOffset > 0 && le.Offset >= options.lastOffset {
			break
		}

		lev := proto.LogEntryValue{}
		if err = lev.UnmarshalVTUnsafe(le.Value); err != nil {
			return err
		}

		if options.json {
			logEntry := LogEntry{
				Term:          le.Term,
				Offset:        le.Offset,
				Timestamp:     time.UnixMilli(int64(le.Timestamp)),
				WriteRequests: lev.GetRequests(),
			}

			ser, err := json.Marshal(logEntry)
			if err != nil {
				return err
			}
			fmt.Println(string(ser))
		} else {
			for _, writes := range lev.GetRequests().Writes {
				printPuts(le, writes.Puts)
				printDeletes(le, writes.Deletes)
				printDeleteRanges(le, writes.DeleteRanges)
			}
		}
	}

	return nil
}

func printPuts(le *proto.LogEntry, puts []*proto.PutRequest) {
	for _, p := range puts {
		args := []any{
			slog.String("op", "put"),
			slog.Time("ts", time.UnixMilli(int64(le.Timestamp))),
			slog.String("key", p.Key),
			slog.Int64("offset", le.Offset),
			slog.Int64("term", le.Term),
		}

		if p.ExpectedVersionId != nil {
			args = append(args, slog.Int64("expected-version-id", p.GetExpectedVersionId()))
		}
		if p.ClientIdentity != nil {
			args = append(args, slog.String("client-identity", p.GetClientIdentity()))
		}
		if p.SessionId != nil {
			args = append(args, slog.Int64("session-id", p.GetSessionId()))
		}

		slog.Info("", args...)
	}
}

func printDeletes(le *proto.LogEntry, deletes []*proto.DeleteRequest) {
	for _, d := range deletes {
		args := []any{
			slog.String("op", "delete"),
			slog.Time("ts", time.UnixMilli(int64(le.Timestamp))),
			slog.String("key", d.Key),
			slog.Int64("offset", le.Offset),
			slog.Int64("term", le.Term),
		}
		if d.ExpectedVersionId != nil {
			args = append(args, slog.Int64("expected-version-i", d.GetExpectedVersionId()))
		}
		slog.Info("", args...)
	}
}

func printDeleteRanges(le *proto.LogEntry, deleteRanges []*proto.DeleteRangeRequest) {
	for _, dr := range deleteRanges {
		slog.Info("",
			slog.String("op", "delete-range"),
			slog.Time("ts", time.UnixMilli(int64(le.Timestamp))),
			slog.String("key-start", dr.StartInclusive),
			slog.String("key-end", dr.EndExclusive),
			slog.Int64("offset", le.Offset),
			slog.Int64("term", le.Term),
		)
	}
}
