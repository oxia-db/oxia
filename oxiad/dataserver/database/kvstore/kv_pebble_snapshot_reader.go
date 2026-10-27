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

package kvstore

import (
	"io"
	"log/slog"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/common/proto"
)

// SnapshotReader reads the files of a snapshot, opened read-only: the values
// are exactly those the files hold.
type SnapshotReader interface {
	io.Closer

	// Get returns the value of a key, or errs.ErrKeyNotFound. Only ComparisonEqual
	// is supported. The caller closes the value.
	Get(key string, comparisonType ComparisonType, opts IteratorOpts) (storedKey string, value []byte, closer io.Closer, err error)
}

// OpenSnapshotReader opens the files of a snapshot read-only, with the key
// encoding their marker records. The files are left as they are.
func OpenSnapshotReader(path string) (SnapshotReader, error) {
	keyEncoder, err := getKeyEncoder(path, proto.KeySortingType_UNKNOWN)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read the key encoding of the snapshot at %s", path)
	}
	db, err := pebble.Open(path, &pebble.Options{
		ReadOnly:           true,
		FS:                 vfs.Default,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		Logger:             &pebbleLogger{slog.With(slog.String("component", "pebble"), slog.String("snapshot", path))},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open the snapshot at %s", path)
	}
	return &pebbleSnapshotReader{db: db, keyEncoder: keyEncoder}, nil
}

type pebbleSnapshotReader struct {
	db         *pebble.DB
	keyEncoder compare.Encoder
}

func (r *pebbleSnapshotReader) Get(key string, comparisonType ComparisonType, _ IteratorOpts) (string, []byte, io.Closer, error) {
	if comparisonType != ComparisonEqual {
		return "", nil, nil, errors.Errorf("a snapshot reader only supports point reads, got comparison %v", comparisonType)
	}
	value, closer, err := r.db.Get(r.keyEncoder.Encode(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil, nil, errs.ErrKeyNotFound
	} else if err != nil {
		return "", nil, nil, err
	}
	return key, value, closer, nil
}

func (r *pebbleSnapshotReader) Close() error {
	return r.db.Close()
}
