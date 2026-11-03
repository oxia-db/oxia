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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"

	// The "mem://" destinations of the tests.
	_ "github.com/oxia-db/oxia/oxiad/backup/blob/blobtest"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// The fakes of the package's tests: the host of the executor, with the
// checkpoints it takes and what their files hold.

var errNotHosted = errors.New("shard not hosted")

type fakeShard struct {
	dir          string
	files        map[string][]byte
	commitOffset int64
	term         int64
	// generation counts the replacements of the database, see replace
	generation uint64
	// publishDir, when set, is the directory WithDatabase hands out instead
	// of dir, for a record that cannot be written
	publishDir string
}

type fakeHost struct {
	t  *testing.T
	mu sync.Mutex
	// shards are the shards the host hosts, by ID
	shards map[int64]*fakeShard
	// checkpointGate, when set, holds every checkpoint until it is closed
	checkpointGate chan struct{}
	checkpoints    int
	// checkpointStates are the commit offset and the term of each checkpoint
	// taken, by directory: what readCheckpoint reads
	checkpointStates map[string]checkpointState
	// publishGate, when set, holds the second WithDatabase call, which is
	// the record publication of the first backup, until it is closed
	publishGate       chan struct{}
	withDatabaseCalls int
	// checkpointErr and withDatabaseErr, when set, fail the calls of the
	// same name
	checkpointErr   error
	withDatabaseErr error
}

type checkpointState struct {
	commitOffset int64
	term         int64
}

func newFakeHost(t *testing.T) *fakeHost {
	t.Helper()
	return &fakeHost{t: t, shards: map[int64]*fakeShard{}, checkpointStates: map[string]checkpointState{}}
}

// readCheckpoint is the Options.ReadCheckpoint of the tests: the commit
// offset and the term of the shard when the checkpoint was taken.
func (h *fakeHost) readCheckpoint(path string) (commitOffset int64, term int64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	state, ok := h.checkpointStates[path]
	if !ok {
		return 0, 0, fmt.Errorf("no checkpoint at %s", path)
	}
	return state.commitOffset, state.term, nil
}

// addShard hosts a shard with the given checkpoint files, at commit offset
// len(files)-1 and term 1.
func (h *fakeHost) addShard(shard int64, files map[string][]byte) *fakeShard {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &fakeShard{dir: h.t.TempDir(), files: files, commitOffset: int64(len(files)) - 1, term: 1}
	h.shards[shard] = s
	return s
}

func (h *fakeHost) write(shard int64, name string, content []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.shards[shard]
	s.files[name] = content
	s.commitOffset++
}

func (h *fakeHost) Checkpoint(shard int64) (*database.Checkpoint, error) {
	h.mu.Lock()
	s, ok := h.shards[shard]
	gate := h.checkpointGate
	checkpointErr := h.checkpointErr
	h.checkpoints++
	h.mu.Unlock()
	if !ok {
		return nil, errNotHosted
	}
	if checkpointErr != nil {
		return nil, checkpointErr
	}
	if gate != nil {
		<-gate
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	dir := filepath.Join(h.t.TempDir(), "checkpoint")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	for name, content := range s.files {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
			return nil, err
		}
	}
	h.checkpointStates[dir] = checkpointState{commitOffset: s.commitOffset, term: s.term}
	return &database.Checkpoint{Snapshot: &fakeSnapshot{dir: dir}, Generation: s.generation}, nil
}

// setPublishDir makes WithDatabase hand out dir for the shard.
func (h *fakeHost) setPublishDir(shard int64, dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.shards[shard].publishDir = dir
}

// replace replaces the database of a shard, as a snapshot install does: a
// new directory and a new generation, with the same files.
func (h *fakeHost) replace(shard int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.shards[shard]
	s.dir = h.t.TempDir()
	s.generation++
}

func (h *fakeHost) WithDatabase(shard int64, generation uint64, fn func(dir string) error) error {
	h.mu.Lock()
	s, ok := h.shards[shard]
	h.withDatabaseCalls++
	gate, gated := h.publishGate, h.withDatabaseCalls == 2
	withDatabaseErr := h.withDatabaseErr
	h.mu.Unlock()
	if !ok {
		return errNotHosted
	}
	if withDatabaseErr != nil {
		return withDatabaseErr
	}
	if gate != nil && gated {
		<-gate
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.generation != generation {
		return fmt.Errorf("%w: the database of the shard was replaced", errs.ErrUnavailable)
	}
	if s.publishDir != "" {
		return fn(s.publishDir)
	}
	return fn(s.dir)
}

func (h *fakeHost) CommitOffset(shard int64) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.shards[shard]
	if !ok {
		return 0, errNotHosted
	}
	return s.commitOffset, nil
}

func (h *fakeHost) DatabaseDir(_ string, shard int64) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.shards[shard]; ok {
		return s.dir
	}
	return filepath.Join(h.t.TempDir(), fmt.Sprintf("shard-%d", shard))
}

type fakeSnapshot struct {
	dir string
}

func (s *fakeSnapshot) BasePath() string { return s.dir }
func (*fakeSnapshot) Valid() bool        { return false }
func (*fakeSnapshot) Next() bool         { return false }
func (*fakeSnapshot) Chunk() (kvstore.SnapshotChunk, error) {
	return nil, errors.New("not implemented")
}
func (s *fakeSnapshot) Close() error { return os.RemoveAll(s.dir) }

// newTestClock returns a clock the tests set, at a fixed start.
func newTestClock() *commontime.MockedClock {
	clock := &commontime.MockedClock{}
	clock.Set(time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC).UnixMilli())
	return clock
}

// advance moves the clock forward by d.
func advance(clock *commontime.MockedClock, d time.Duration) {
	clock.Set(clock.Now().Add(d).UnixMilli())
}

// sstFiles returns checkpoint files: count SSTs of random-ish content, and
// the usual small files.
func sstFiles(count int) map[string][]byte {
	files := map[string][]byte{
		"MANIFEST-000001":          []byte("manifest"),
		"OPTIONS-000003":           []byte("options"),
		"oxia-key-encoding-format": []byte("natural"),
		"marker.manifest.000001":   {},
	}
	for i := range count {
		files[fmt.Sprintf("%06d.sst", i+1)] = bytes.Repeat([]byte{byte(i + 1)}, 1000*(i+1))
	}
	return files
}

func testPolicy() policy {
	return policy{destination: "mem://unused", interval: time.Hour, retention: 7 * 24 * time.Hour, maxConcurrent: 2}
}

func protoPolicy(destination string) *proto.BackupPolicy {
	return &proto.BackupPolicy{
		Destination:            destination,
		IntervalMillis:         time.Hour.Milliseconds(),
		RetentionMillis:        (7 * 24 * time.Hour).Milliseconds(),
		MaxConcurrentPerServer: 2,
	}
}

func readObject(ctx context.Context, t *testing.T, store interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}, key string) []byte {
	t.Helper()
	r, err := store.Get(ctx, key)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

func fullRange() *proto.HashRange {
	return &proto.HashRange{Min: backup.FullRange.Min, Max: backup.FullRange.Max}
}

// backupSeries takes a backup of the shard every interval from start, count
// times, with the store stamping the objects at the backup's time.
func backupSeries(t *testing.T, store *blob.Memory, host *fakeHost, shard int64, start time.Time,
	interval time.Duration, count int) []*LastBackup {
	t.Helper()
	record := &Record{Lineage: "l" + strings.Repeat("0", 14) + string(rune('a'+shard))}
	var backups []*LastBackup
	for i := range count {
		now := start.Add(time.Duration(i) * interval)
		store.SetClock(func() time.Time { return now })
		host.write(shard, "00000"+string(rune('1'+i))+".sst", []byte(strings.Repeat("x", 100*(i+1))))
		last, err := runBackup(t, store, host, shard, record, testPolicy(), now)
		require.NoError(t, err)
		record.LastBackup = last
		backups = append(backups, last)
	}
	return backups
}

func manifestKeys(store *blob.Memory) []string {
	var keys []string
	for _, key := range store.Keys() {
		if strings.HasSuffix(key, ".json") {
			keys = append(keys, key)
		}
	}
	return keys
}
