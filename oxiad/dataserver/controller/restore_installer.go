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

package controller

import (
	"io"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// Installer installs the database of a shard from its files, for a restore
// from a backup: the files are written through the snapshot loader, which
// makes them durable, and the database is then opened once to filter it to
// the hash range of the shard and read its commit offset and term. The next
// term of the shard opens it as usual.
type Installer interface {
	// AddFile writes a file of the database, of the given size.
	AddFile(name string, size int64, r io.Reader) error
	// Complete makes the files durable, filters the database to the hash
	// range, and returns its commit offset and term.
	Complete(hashRange *proto.HashRange) (commitOffset int64, term int64, err error)
	// Close deletes the files of an installation that didn't complete.
	Close() error
}

type installer struct {
	storageOptions *option.StorageOptions
	namespace      string
	shardId        int64
	kvFactory      kvstore.Factory
	loader         kvstore.SnapshotLoader
	completed      bool
}

// newInstaller clears the WAL of the shard, which the restored database is
// ahead of, and prepares the snapshot loader, which wipes the database.
func newInstaller(storageOptions *option.StorageOptions, namespace string, shardId int64,
	walFactory wal.Factory, kvFactory kvstore.Factory) (Installer, error) {
	shardWal, err := walFactory.NewWal(namespace, shardId, noCommitOffset{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open the WAL")
	}
	if err := multierr.Combine(shardWal.Clear(), shardWal.Close()); err != nil {
		return nil, errors.Wrap(err, "failed to clear the WAL")
	}
	loader, err := kvFactory.NewSnapshotLoader(namespace, shardId)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the snapshot loader")
	}
	return &installer{
		storageOptions: storageOptions,
		namespace:      namespace,
		shardId:        shardId,
		kvFactory:      kvFactory,
		loader:         loader,
	}, nil
}

func (i *installer) AddFile(name string, size int64, r io.Reader) error {
	chunkCount := int32(max(1, (size+kvstore.MaxSnapshotChunkSize-1)/kvstore.MaxSnapshotChunkSize))
	buf := make([]byte, kvstore.MaxSnapshotChunkSize)
	remaining := size
	for index := range chunkCount {
		chunk := buf[:min(remaining, kvstore.MaxSnapshotChunkSize)]
		if _, err := io.ReadFull(r, chunk); err != nil {
			return errors.Wrapf(err, "failed to read %s: want %d bytes", name, size)
		}
		remaining -= int64(len(chunk))
		if err := i.loader.AddChunk(name, index, chunkCount, chunk); err != nil {
			return errors.Wrapf(err, "failed to write %s", name)
		}
	}
	if _, err := r.Read(buf[:1]); !errors.Is(err, io.EOF) {
		return errors.Errorf("%s is longer than its size %d", name, size)
	}
	return nil
}

func (i *installer) Complete(hashRange *proto.HashRange) (commitOffset int64, term int64, err error) {
	if err := i.loader.Complete(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to complete the snapshot")
	}
	i.completed = true
	db, err := database.NewDB(i.namespace, i.shardId, i.kvFactory, proto.KeySortingType_UNKNOWN,
		i.storageOptions.Notification.Retention.ToDuration(), commontime.SystemClock)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to open the database")
	}
	commitOffset, term, err = filterRestoredDatabase(db, hashRange)
	return commitOffset, term, multierr.Append(err, db.Close())
}

// filterRestoredDatabase deletes the records outside the hash range: the
// backup may be of a shard that covered more, and a split child's may hold
// records of its parent.
func filterRestoredDatabase(db database.DB, hashRange *proto.HashRange) (commitOffset int64, term int64, err error) {
	if err := database.FilterDBForSplit(db.RawKV(), hashRange); err != nil {
		return 0, 0, errors.Wrap(err, "failed to filter the database")
	}
	if err := database.ClearSplitFilters(db.RawKV()); err != nil {
		return 0, 0, err
	}
	if commitOffset, err = db.ReadCommitOffset(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to read the commit offset")
	}
	if term, _, err = db.ReadTerm(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to read the term")
	}
	return commitOffset, term, nil
}

func (i *installer) Close() error {
	if i.completed {
		return nil
	}
	return i.loader.Close()
}

// noCommitOffset is the commit offset provider of a WAL opened only to be
// cleared: nothing is applied from it.
type noCommitOffset struct{}

func (noCommitOffset) CommitOffset() int64 {
	return constant.I64NegativeOne
}

func (noCommitOffset) FlushDatabase() error {
	return nil
}
