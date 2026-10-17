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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// The fakes of the package's tests: the host of the executor, with the
// checkpoints it takes and the installers it hands out.

var errNotHosted = errors.New("shard not hosted")

type fakeShard struct {
	dir          string
	files        map[string][]byte
	commitOffset int64
	term         int64
	// generation counts the replacements of the database, see replace
	generation uint64
}

type fakeHost struct {
	t  *testing.T
	mu sync.Mutex
	// shards are the shards the host hosts, by ID
	shards map[int64]*fakeShard
	// checkpointGate, when set, holds every checkpoint until it is closed
	checkpointGate chan struct{}
	checkpoints    int
	// publishGate, when set, holds the second WithDatabase call, which is
	// the record publication of the first backup, until it is closed
	publishGate       chan struct{}
	withDatabaseCalls int
	installers        map[int64]*fakeInstaller
	installerErr      error
}

func newFakeHost(t *testing.T) *fakeHost {
	t.Helper()
	return &fakeHost{t: t, shards: map[int64]*fakeShard{}, installers: map[int64]*fakeInstaller{}}
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
	h.checkpoints++
	h.mu.Unlock()
	if !ok {
		return nil, errNotHosted
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
	return &database.Checkpoint{Snapshot: &fakeSnapshot{dir: dir}, CommitOffset: s.commitOffset, Term: s.term,
		Generation: s.generation}, nil
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
	h.mu.Unlock()
	if !ok {
		return errNotHosted
	}
	if gate != nil && gated {
		<-gate
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.generation != generation {
		return database.ErrDatabaseReplaced
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

func (h *fakeHost) NewInstaller(_ string, shard int64) (controller.Installer, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.installerErr != nil {
		return nil, h.installerErr
	}
	installer := &fakeInstaller{files: map[string][]byte{}, commitOffset: 42, term: 3}
	h.installers[shard] = installer
	return installer, nil
}

func (h *fakeHost) DatabaseDir(_ string, shard int64) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.shards[shard]; ok {
		return s.dir
	}
	return filepath.Join(h.t.TempDir(), fmt.Sprintf("shard-%d", shard))
}

func (h *fakeHost) installer(shard int64) *fakeInstaller {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.installers[shard]
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

type fakeInstaller struct {
	mu           sync.Mutex
	files        map[string][]byte
	hashRange    *proto.HashRange
	completed    bool
	closed       bool
	commitOffset int64
	term         int64
}

func (i *fakeInstaller) AddFile(name string, size int64, r io.Reader) error {
	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if int64(len(content)) != size {
		return fmt.Errorf("%s: %d bytes, want %d", name, len(content), size)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.files[name] = content
	return nil
}

func (i *fakeInstaller) Complete(hashRange *proto.HashRange) (commitOffset int64, term int64, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.hashRange = hashRange
	i.completed = true
	return i.commitOffset, i.term, nil
}

func (i *fakeInstaller) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	return nil
}

// testClock is a clock the tests advance.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
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
