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

package database

import (
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// Checkpoint is a snapshot of the files of a database, taken by the
// controller of its shard. Closing it deletes the files. The commit offset
// and the term are in the files: ReadCheckpoint reads them.
type Checkpoint struct {
	kvstore.Snapshot
	// Generation is the generation of the database the checkpoint was taken
	// from (kvstore.Factory.DatabaseGeneration), set by the controller that
	// took it: the WithDatabase of the controller compares it.
	Generation uint64
}

// ReadCheckpoint returns the commit offset and the term the files of a
// checkpoint hold: the last entry applied to them, exactly, and the term
// they were written under. A database without a commit yet reports
// wal.InvalidOffset, and one without a term wal.InvalidTerm.
func ReadCheckpoint(path string) (commitOffset int64, term int64, err error) {
	reader, err := kvstore.OpenSnapshotReader(path)
	if err != nil {
		return wal.InvalidOffset, wal.InvalidTerm, errors.Wrap(err, "failed to open the checkpoint")
	}
	defer func() { err = multierr.Append(err, reader.Close()) }()
	if commitOffset, err = readASCIILongOrDefault(reader, commitOffsetKey, constant.I64NegativeOne); err != nil {
		return wal.InvalidOffset, wal.InvalidTerm, errors.Wrap(err, "failed to read the commit offset of the checkpoint")
	}
	if term, err = readASCIILongOrDefault(reader, termKey, wal.InvalidTerm); err != nil {
		return wal.InvalidOffset, wal.InvalidTerm, errors.Wrap(err, "failed to read the term of the checkpoint")
	}
	return commitOffset, term, nil
}
