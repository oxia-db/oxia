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

// Package backup is the core of the shard backups: the layout of a backup in
// the object store, the manifest that describes it, the retention rule that
// expires the old ones, and the planning of a restore. It holds no server
// state: the data servers take and restore the backups (package
// oxiad/dataserver/backup), and the coordinator schedules them (package
// oxiad/coordinator/runtime/backup).
package backup

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	// idTimeLayout is the time part of a backup ID: UTC, to the second, with
	// characters that every object store accepts in a key.
	idTimeLayout = "2006-01-02T15-04-05Z"
	lineageBytes = 8
	manifestExt  = ".json"
	sstExt       = ".sst"
	// CompressedExt is the suffix of a compressed SST object.
	CompressedExt = ".zst"
	shardDir      = "shard-"
	sstDir        = "sst/"
	backupsDir    = "backups/"
	separator     = "/"
)

// ErrInvalidID is returned when a backup ID doesn't parse.
var ErrInvalidID = errors.New("backup: invalid backup id")

// NewLineage returns a new random lineage: the identity of one database of
// one replica, which the SST object keys of its backups carry.
func NewLineage() (string, error) {
	var b [lineageBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("backup: generate lineage: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ID identifies a backup of a shard: its start time, to the second, and the
// lineage of the replica that took it. Backups are ordered by time, then by
// lineage, so that two backups never tie.
type ID struct {
	Time    time.Time
	Lineage string
}

// NewID returns the ID of a backup that starts at t.
func NewID(t time.Time, lineage string) ID {
	return ID{Time: t.UTC().Truncate(time.Second), Lineage: lineage}
}

// ParseID parses an ID formatted by String.
func ParseID(s string) (ID, error) {
	sep := strings.LastIndexByte(s, '-')
	if sep <= 0 || sep == len(s)-1 {
		return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
	}
	t, err := time.Parse(idTimeLayout, s[:sep])
	if err != nil {
		return ID{}, fmt.Errorf("%w: %q: %w", ErrInvalidID, s, err)
	}
	return ID{Time: t, Lineage: s[sep+1:]}, nil
}

// String formats the ID as "<time>-<lineage>", e.g.
// "2026-10-03T14-25-07Z-5f3a9c1e2b7d4e60".
func (id ID) String() string {
	return id.Time.UTC().Format(idTimeLayout) + "-" + id.Lineage
}

// Compare orders the IDs by time, then by lineage.
func (id ID) Compare(other ID) int {
	if c := id.Time.Compare(other.Time); c != 0 {
		return c
	}
	return strings.Compare(id.Lineage, other.Lineage)
}

// Range is a range of the hash space, both bounds included.
type Range struct {
	Min uint32 `json:"min"`
	Max uint32 `json:"max"`
}

// FullRange is the whole hash space.
var FullRange = Range{Min: 0, Max: math.MaxUint32}

// Covers reports whether r includes every point of other.
func (r Range) Covers(other Range) bool {
	return r.Min <= other.Min && other.Max <= r.Max
}

// Overlaps reports whether r and other share a point.
func (r Range) Overlaps(other Range) bool {
	return r.Min <= other.Max && other.Min <= r.Max
}

func (r Range) String() string {
	return fmt.Sprintf("[%d, %d]", r.Min, r.Max)
}

// ShardPath locates the backups of a shard in the store: every key of the
// shard starts with its Prefix.
type ShardPath struct {
	Namespace string
	Shard     int64
}

// NamespacePrefix is the prefix of the keys of every shard of a namespace.
func NamespacePrefix(namespace string) string {
	return namespace + separator
}

// ParseShardDir returns the shard of a directory that ListDirs returned under
// NamespacePrefix, e.g. "ns/shard-12/", and false for any other directory.
func ParseShardDir(dir string) (ShardPath, bool) {
	dir = strings.TrimSuffix(dir, separator)
	namespace, base := path.Split(dir)
	namespace = strings.TrimSuffix(namespace, separator)
	if namespace == "" || !strings.HasPrefix(base, shardDir) {
		return ShardPath{}, false
	}
	shard, err := strconv.ParseInt(strings.TrimPrefix(base, shardDir), 10, 64)
	if err != nil || shard < 0 {
		return ShardPath{}, false
	}
	return ShardPath{Namespace: namespace, Shard: shard}, true
}

// Prefix is the prefix of every key of the shard.
func (p ShardPath) Prefix() string {
	return fmt.Sprintf("%s%s%d%s", NamespacePrefix(p.Namespace), shardDir, p.Shard, separator)
}

// SSTPrefix is the prefix of the SST objects of the shard, shared by its
// backups.
func (p ShardPath) SSTPrefix() string {
	return p.Prefix() + sstDir
}

// SSTKey is the key of an SST file of a database, by the file name Pebble
// gave it, e.g. "000123.sst", stored with the given compression (see
// Manifest.Compression): a key that ends with CompressedExt holds the
// zstd-compressed file.
func (p ShardPath) SSTKey(lineage string, name string, compression string) string {
	key := p.SSTPrefix() + lineage + "-" + name
	if compression == CompressionZstd {
		key += CompressedExt
	}
	return key
}

// IsCompressedKey reports whether the object at key holds a compressed file.
func IsCompressedKey(key string) bool {
	return strings.HasSuffix(key, CompressedExt)
}

// ManifestsPrefix is the prefix of the manifests, and of the per-backup
// files, of the shard. A date narrows it to the backups of a day, e.g.
// ManifestsPrefix() + "2026-10-03".
func (p ShardPath) ManifestsPrefix() string {
	return p.Prefix() + backupsDir
}

// ManifestKey is the key of the manifest of a backup. The manifest is written
// last: the backup exists once it does.
func (p ShardPath) ManifestKey(id ID) string {
	return p.ManifestsPrefix() + id.String() + manifestExt
}

// FilesPrefix is the prefix of the per-backup files of a backup: the files
// of the checkpoint other than the SSTs.
func (p ShardPath) FilesPrefix(id ID) string {
	return p.ManifestsPrefix() + id.String() + separator
}

// FileKey is the key of a per-backup file.
func (p ShardPath) FileKey(id ID, name string) string {
	return p.FilesPrefix(id) + name
}

// ParseManifestKey returns the backup of a manifest key, and false for any
// other key.
func ParseManifestKey(key string) (ShardPath, ID, bool) {
	dir, base := path.Split(key)
	shardPath, ok := ParseShardDir(strings.TrimSuffix(dir, backupsDir))
	if !ok || !strings.HasSuffix(dir, backupsDir) || !strings.HasSuffix(base, manifestExt) {
		return ShardPath{}, ID{}, false
	}
	id, err := ParseID(strings.TrimSuffix(base, manifestExt))
	if err != nil {
		return ShardPath{}, ID{}, false
	}
	return shardPath, id, true
}

// IsSSTName reports whether a checkpoint file is an SST, which goes to the
// shared pool of the shard, rather than a per-backup file.
func IsSSTName(name string) bool {
	return strings.HasSuffix(name, sstExt)
}
