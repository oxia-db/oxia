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

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// ErrDatabaseReplaced is returned by the WithDatabase of a controller when
// the database was replaced or deleted since the checkpoint it was given.
var ErrDatabaseReplaced = errors.New("the database of the shard was replaced")

// Checkpoint is a snapshot of the files of a database, with the commit
// offset and the term read right before it was taken. Closing it deletes
// the files.
type Checkpoint struct {
	kvstore.Snapshot
	// CommitOffset is the last entry the snapshot is sure to hold: it may
	// hold a few more, applied between the read and the snapshot.
	CommitOffset int64
	Term         int64
	// Generation is the generation of the database the checkpoint was taken
	// from (kvstore.Factory.DatabaseGeneration), set by the controller that
	// took it: the WithDatabase of the controller compares it.
	Generation uint64
}

func (d *db) Checkpoint() (*Checkpoint, error) {
	commitOffset, err := d.ReadCommitOffset()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read the commit offset")
	}
	term, _, err := d.ReadTerm()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read the term")
	}
	snapshot, err := d.kv.Snapshot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to take the snapshot")
	}
	return &Checkpoint{Snapshot: snapshot, CommitOffset: commitOffset, Term: term}, nil
}
