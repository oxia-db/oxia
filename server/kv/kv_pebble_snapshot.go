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

package kv

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type pebbleSnapshot struct {
	path       string
	files      []string
	chunkCount int32
	chunkIndex int32
	file       *os.File
}

type pebbleSnapshotChunk struct {
	name       string
	index      int32
	totalCount int32
	content    []byte
}

func newPebbleSnapshot(p *Pebble) (Snapshot, error) {
	ps := &pebbleSnapshot{
		path: filepath.Join(p.dataDir, "snapshots",
			fmt.Sprintf("shard-%d", p.shardId),
			fmt.Sprintf("snapshot-%d", p.snapshotCounter.Add(1))),
	}

	// Flush the DB to ensure the snapshot content is as close as possible to
	// the last committed entry
	if err := p.db.Flush(); err != nil {
		return nil, err
	}

	if err := p.db.Checkpoint(ps.path); err != nil {
		return nil, err
	}

	dirEntries, err := os.ReadDir(ps.path)
	if err != nil {
		return nil, err
	}

	for _, de := range dirEntries {
		if !de.IsDir() {
			ps.files = append(ps.files, de.Name())
		}
	}

	return ps, nil
}

func (ps *pebbleSnapshot) Close() error {
	var err error
	if ps.file != nil {
		err = ps.file.Close()
	}
	return multierr.Combine(err, os.RemoveAll(ps.path))
}

func (ps *pebbleSnapshot) BasePath() string {
	return ps.path
}

func (ps *pebbleSnapshot) Valid() bool {
	return len(ps.files) > 0
}

func (ps *pebbleSnapshot) Next() bool {
	ps.chunkIndex++
	if ps.chunkIndex == ps.chunkCount {
		ps.chunkIndex = 0
		ps.files = ps.files[1:]
	}
	return ps.Valid()
}

func (ps *pebbleSnapshot) Chunk() (SnapshotChunk, error) {
	content, err := ps.NextChunkContent()
	if err != nil {
		return nil, err
	}
	return &pebbleSnapshotChunk{
		ps.files[0],
		ps.chunkIndex,
		ps.chunkCount,
		content}, nil
}

func (psf *pebbleSnapshotChunk) Name() string {
	return psf.name
}

func (psf *pebbleSnapshotChunk) Content() []byte {
	return psf.content
}

func (psf *pebbleSnapshotChunk) Index() int32 {
	return psf.index
}

func (psf *pebbleSnapshotChunk) TotalCount() int32 {
	return psf.totalCount
}

func (ps *pebbleSnapshot) initalizeChunkContent() error {
	var err error
	filePath := filepath.Join(ps.path, ps.files[0])
	stat, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	ps.chunkCount = int32(fileSize / MaxSnapshotChunkSize)
	if fileSize%MaxSnapshotChunkSize != 0 {
		ps.chunkCount++
	}
	if ps.chunkCount == 0 {
		// empty file
		ps.chunkCount = 1
	}

	ps.file, err = os.Open(filePath)
	if err != nil {
		return err
	}

	return nil
}

func (ps *pebbleSnapshot) NextChunkContent() ([]byte, error) {
	if ps.chunkIndex == 0 {
		err := ps.initalizeChunkContent()
		if err != nil {
			return nil, err
		}
	}

	_, err := ps.file.Seek(int64(ps.chunkIndex)*MaxSnapshotChunkSize, io.SeekStart)
	if err != nil {
		return nil, err
	}
	content := make([]byte, MaxSnapshotChunkSize)
	byteCount, err := io.ReadFull(ps.file, content)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if int64(byteCount) < MaxSnapshotChunkSize {
		content = content[:byteCount]

		err = ps.file.Close()
		if err != nil {
			return nil, err
		}
		ps.file = nil
	}
	return content, nil
}
