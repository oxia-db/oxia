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

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// ManifestFormatVersion is the version of the manifest format that this code
// writes, and the only one it reads.
const ManifestFormatVersion = 1

// ErrInvalidManifest is returned for a manifest that doesn't describe a
// backup this code can restore.
var ErrInvalidManifest = errors.New("backup: invalid manifest")

// File is a file of the checkpoint that a backup holds, with the key of the
// object that stores it. An SST is in the shared pool of the shard, any other
// file is stored per backup.
type File struct {
	// Name is the name the file has in the database directory.
	Name string `json:"name"`
	// Key is the object that holds the file.
	Key string `json:"key"`
	// Size is the size of the file, in bytes.
	Size int64 `json:"size"`
	// StoredSize is the size of the object, which differs from Size when the
	// object is compressed.
	StoredSize int64 `json:"stored_size"`
	// CRC32C is the checksum of the file (the Castagnoli polynomial).
	CRC32C uint32 `json:"crc32c"`
}

// Manifest describes a backup. It is written last, once every file of the
// backup is stored, and never changes afterwards.
type Manifest struct {
	FormatVersion int `json:"format_version"`
	// ID is the backup ID, see ID.
	ID string `json:"id"`
	// Lineage is the database the backup was taken from, see NewLineage.
	Lineage   string `json:"lineage"`
	Namespace string `json:"namespace"`
	Shard     int64  `json:"shard"`
	// Range is the hash range of the shard when the backup was taken.
	Range Range `json:"hash_range"`
	// NamespaceConfig is the configuration of the namespace when the backup
	// was taken, in the protobuf JSON format, for a restore to recreate it.
	NamespaceConfig json.RawMessage `json:"namespace_config,omitempty"`
	Term            int64           `json:"term"`
	// CommitOffset is the commit offset read right before the checkpoint: the
	// backup holds every entry up to it, and may hold a few more.
	CommitOffset int64     `json:"commit_offset"`
	CreatedAt    time.Time `json:"created_at"`
	// Compression names the compression of the SST objects: "zstd", or empty
	// when they are stored as they are.
	Compression string `json:"compression,omitempty"`
	SSTs        []File `json:"ssts"`
	Files       []File `json:"files"`
}

// Validate checks that the manifest describes a backup this code can
// restore.
func (m *Manifest) Validate() error {
	switch {
	case m.FormatVersion != ManifestFormatVersion:
		return fmt.Errorf("%w: format version %d, want %d", ErrInvalidManifest, m.FormatVersion, ManifestFormatVersion)
	case m.Namespace == "" || m.Shard < 0 || m.Lineage == "":
		return fmt.Errorf("%w: missing namespace, shard or lineage", ErrInvalidManifest)
	case m.Range.Min > m.Range.Max:
		return fmt.Errorf("%w: hash range %s", ErrInvalidManifest, m.Range)
	case m.Compression != "" && m.Compression != CompressionZstd:
		return fmt.Errorf("%w: unknown compression %q", ErrInvalidManifest, m.Compression)
	}
	id, err := ParseID(m.ID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}
	if id.Lineage != m.Lineage {
		return fmt.Errorf("%w: id %q is not of lineage %q", ErrInvalidManifest, m.ID, m.Lineage)
	}
	for _, f := range slices.Concat(m.SSTs, m.Files) {
		if f.Name == "" || f.Key == "" || f.Size < 0 {
			return fmt.Errorf("%w: invalid file %+v", ErrInvalidManifest, f)
		}
	}
	return nil
}

// CompressionZstd is the Compression of a manifest whose SST objects are
// zstd-compressed.
const CompressionZstd = "zstd"

// Summary is what the retention rule, the restore planning and the listings
// need of a backup: its manifest without the file lists.
type Summary struct {
	Namespace string
	Shard     int64
	ID        ID
	// CreatedAt is when the backup started, with more precision than the
	// time of the ID: a restore as of a time picks the backups started at or
	// before it.
	CreatedAt    time.Time
	Range        Range
	Term         int64
	CommitOffset int64
	// SizeBytes is the size of the files of the backup, before compression.
	SizeBytes int64
	SSTCount  int
	// NamespaceConfig is the configuration of the namespace recorded in the
	// manifest, for a restore to recreate it.
	NamespaceConfig json.RawMessage
}

// Summary returns the summary of the manifest, which must be valid.
func (m *Manifest) Summary() Summary {
	id, _ := ParseID(m.ID)
	s := Summary{
		Namespace:       m.Namespace,
		Shard:           m.Shard,
		ID:              id,
		CreatedAt:       m.CreatedAt,
		Range:           m.Range,
		Term:            m.Term,
		CommitOffset:    m.CommitOffset,
		SSTCount:        len(m.SSTs),
		NamespaceConfig: m.NamespaceConfig,
	}
	for _, f := range m.SSTs {
		s.SizeBytes += f.Size
	}
	for _, f := range m.Files {
		s.SizeBytes += f.Size
	}
	return s
}

// StartedAt is when the backup started: CreatedAt, or the time of the ID
// when the manifest doesn't record it.
func (s Summary) StartedAt() time.Time {
	if s.CreatedAt.IsZero() {
		return s.ID.Time
	}
	return s.CreatedAt
}

// Path is the location of the backup in the store.
func (s Summary) Path() ShardPath {
	return ShardPath{Namespace: s.Namespace, Shard: s.Shard}
}

// ManifestKey is the key of the manifest of the backup.
func (s Summary) ManifestKey() string {
	return s.Path().ManifestKey(s.ID)
}

// Newer reports whether s was taken after other, in the order of ID.Compare.
func (s Summary) Newer(other Summary) bool {
	return s.ID.Compare(other.ID) > 0
}

// WriteManifest stores the manifest, which must be valid, at its key.
func WriteManifest(ctx context.Context, store blob.Store, m *Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("backup: encode manifest: %w", err)
	}
	id, _ := ParseID(m.ID)
	key := ShardPath{Namespace: m.Namespace, Shard: m.Shard}.ManifestKey(id)
	if err := store.Put(ctx, key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("backup: write manifest %s: %w", key, err)
	}
	return nil
}

// ReadManifest reads and validates the manifest at key. It returns
// blob.ErrNotFound when there is none.
func ReadManifest(ctx context.Context, store blob.Store, key string) (*Manifest, error) {
	r, err := store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("backup: read manifest %s: %w", key, err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("backup: read manifest %s: %w", key, err)
	}
	m := &Manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrInvalidManifest, key, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, key)
	}
	return m, nil
}

// ListManifests returns the summaries of the backups of a shard, oldest
// first. It reads every manifest of the shard.
func ListManifests(ctx context.Context, store blob.Store, p ShardPath) ([]Summary, error) {
	var keys []string
	err := store.List(ctx, p.ManifestsPrefix(), func(object blob.Object) error {
		if _, _, ok := ParseManifestKey(object.Key); ok {
			keys = append(keys, object.Key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backup: list manifests of %s: %w", p.Prefix(), err)
	}
	summaries := make([]Summary, 0, len(keys))
	for _, key := range keys {
		m, err := ReadManifest(ctx, store, key)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, m.Summary())
	}
	return summaries, nil
}

// ListShards returns the shards that have backups in a namespace.
func ListShards(ctx context.Context, store blob.Store, namespace string) ([]ShardPath, error) {
	dirs, err := store.ListDirs(ctx, NamespacePrefix(namespace))
	if err != nil {
		return nil, fmt.Errorf("backup: list shards of %s: %w", namespace, err)
	}
	shards := make([]ShardPath, 0, len(dirs))
	for _, dir := range dirs {
		if p, ok := ParseShardDir(dir); ok {
			shards = append(shards, p)
		}
	}
	return shards, nil
}
