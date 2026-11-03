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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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

// The fakes of the package's tests: the host of the executor, whose shards
// are databases of their own, so that their checkpoints are the real thing.
//
// A shard holds one SST until a test writes to it: the next checkpoint
// flushes the writes into another, which Pebble compacts with the first
// soon after. A test that writes to a shard it backed up derives what the
// next backup shares from the manifests, not from the SSTs it expects.

const testNamespace = "ns"

var errNotHosted = errors.New("shard not hosted")

// shardDatabase is the database of a fake shard, in a directory of its own.
type shardDatabase struct {
	factory kvstore.Factory
	db      database.DB
	once    sync.Once
}

func (d *shardDatabase) close(t *testing.T) {
	t.Helper()
	d.once.Do(func() {
		require.NoError(t, d.db.Close())
		require.NoError(t, d.factory.Close())
	})
}

type fakeShard struct {
	*shardDatabase
	// writes are the values written to the database, one per commit offset;
	// replace writes them again to the new database
	writes [][]byte
	// generation counts the replacements of the database, see replace
	generation uint64
	// publishDir, when set, is the directory WithDatabase hands out instead
	// of the database's, for a record that cannot be written
	publishDir string
}

type fakeHost struct {
	t  *testing.T
	mu sync.Mutex
	// shards are the shards the host hosts, by ID
	shards map[int64]*fakeShard
	// checkpointGate, when set, holds every checkpoint until it is closed
	checkpointGate chan struct{}
	// afterCheckpoint, when set, runs on the directory of every checkpoint
	// taken, before the checkpoint is returned
	afterCheckpoint func(dir string)
	// publishGate, when set, holds the second WithDatabase call, which is
	// the record publication of the first backup, until it is closed
	publishGate       chan struct{}
	withDatabaseCalls int
	// checkpointErr and withDatabaseErr, when set, fail the calls of the
	// same name
	checkpointErr   error
	withDatabaseErr error
}

func newFakeHost(t *testing.T) *fakeHost {
	t.Helper()
	return &fakeHost{t: t, shards: map[int64]*fakeShard{}}
}

// addShard hosts a shard with writes values in its database, at commit
// offset writes-1 and term 1, in one SST.
func (h *fakeHost) addShard(shard int64, writes int) *fakeShard {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &fakeShard{}
	for i := range writes {
		s.writes = append(s.writes, fmt.Appendf(nil, "value %d", i))
	}
	h.open(shard, s)
	h.shards[shard] = s
	return s
}

// open opens a database for the shard, in a directory of its own, closed
// when the test ends, and writes the shard's values to it, then its term:
// updating the term flushes them all into one SST. Notifications are off,
// as the term options of a leader can leave them: their trimmer writes to
// the database on its own otherwise.
func (h *fakeHost) open(shard int64, s *fakeShard) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(h.t))
	require.NoError(h.t, err)
	db, err := database.NewDB(testNamespace, shard, factory, proto.KeySortingType_NATURAL, time.Hour,
		commontime.SystemClock)
	require.NoError(h.t, err)
	db.EnableNotifications(false)
	s.shardDatabase = &shardDatabase{factory: factory, db: db}
	h.t.Cleanup(func() { s.close(h.t) })
	h.apply(s)
	require.NoError(h.t, db.UpdateTerm(1, database.TermOptions{}))
}

// apply writes the values of the shard its database doesn't hold yet, one
// per commit offset.
func (h *fakeHost) apply(s *fakeShard) {
	commitOffset, err := s.db.ReadCommitOffset()
	require.NoError(h.t, err)
	for offset := commitOffset + 1; offset < int64(len(s.writes)); offset++ {
		put := &proto.PutRequest{Key: fmt.Sprintf("key-%d", offset), Value: s.writes[offset]}
		_, err := s.db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{put}}, offset, 0, database.NoOpCallback)
		require.NoError(h.t, err)
	}
}

// write writes a value to the shard, at the next commit offset.
func (h *fakeHost) write(shard int64, value []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.shards[shard]
	s.writes = append(s.writes, value)
	h.apply(s)
}

// files returns the files of a checkpoint of the shard, by name: what a
// backup of it holds.
func (h *fakeHost) files(shard int64) map[string][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot, err := h.shards[shard].db.Snapshot()
	require.NoError(h.t, err)
	defer func() { require.NoError(h.t, snapshot.Close()) }()
	entries, err := os.ReadDir(snapshot.BasePath())
	require.NoError(h.t, err)
	files := map[string][]byte{}
	for _, entry := range entries {
		content, err := os.ReadFile(filepath.Join(snapshot.BasePath(), entry.Name()))
		require.NoError(h.t, err)
		files[entry.Name()] = content
	}
	return files
}

// sstNames returns the names of the SSTs among files, in order.
func sstNames(files map[string][]byte) []string {
	var names []string
	for name := range files {
		if backup.IsSSTName(name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func (h *fakeHost) Checkpoint(shard int64) (*database.Checkpoint, error) {
	h.mu.Lock()
	s, ok := h.shards[shard]
	gate := h.checkpointGate
	checkpointErr := h.checkpointErr
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
	snapshot, err := s.db.Snapshot()
	if err != nil {
		return nil, err
	}
	if h.afterCheckpoint != nil {
		h.afterCheckpoint(snapshot.BasePath())
	}
	return &database.Checkpoint{Snapshot: snapshot, Generation: s.generation}, nil
}

// setPublishDir makes WithDatabase hand out dir for the shard.
func (h *fakeHost) setPublishDir(shard int64, dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.shards[shard].publishDir = dir
}

// replace replaces the database of a shard, as a snapshot install does: a
// new directory and a new generation, with the same writes.
func (h *fakeHost) replace(shard int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.shards[shard]
	s.close(h.t)
	h.open(shard, s)
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
	return fn(s.factory.DatabaseDir(testNamespace, shard))
}

func (h *fakeHost) CommitOffset(shard int64) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.shards[shard]
	if !ok {
		return 0, errNotHosted
	}
	return s.db.ReadCommitOffset()
}

func (h *fakeHost) DatabaseDir(namespace string, shard int64) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.shards[shard]; ok {
		return s.factory.DatabaseDir(namespace, shard)
	}
	return filepath.Join(h.t.TempDir(), fmt.Sprintf("shard-%d", shard))
}

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
