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

package metadata

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSnapshotSink struct {
	data       []byte
	writeErr   error
	shortWrite bool
	closed     bool
	cancelled  bool
}

func (*testSnapshotSink) ID() string {
	return "test-snapshot"
}

func (s *testSnapshotSink) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	if s.shortWrite {
		return len(p) - 1, nil
	}
	s.data = append(s.data, p...)
	return len(p), nil
}

func (s *testSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *testSnapshotSink) Cancel() error {
	s.cancelled = true
	return nil
}

func TestStateContainerSnapshotPersistsNilState(t *testing.T) {
	sc := newStateContainer(slog.Default())

	snapshot, err := sc.Snapshot()
	require.NoError(t, err)

	sink := &testSnapshotSink{}
	require.NoError(t, snapshot.Persist(sink))

	assert.True(t, sink.closed)
	assert.False(t, sink.cancelled)
	assert.JSONEq(t, `{"state":null,"current_version":-1}`, string(sink.data))
}

func TestStateContainerPersistCancelsOnWriteError(t *testing.T) {
	sc := newStateContainer(slog.Default())
	snapshot, err := sc.Snapshot()
	require.NoError(t, err)

	sink := &testSnapshotSink{writeErr: errors.New("write failed")}
	err = snapshot.Persist(sink)

	require.Error(t, err)
	assert.True(t, sink.cancelled)
	assert.False(t, sink.closed)
}

func TestStateContainerPersistCancelsOnShortWrite(t *testing.T) {
	sc := newStateContainer(slog.Default())
	snapshot, err := sc.Snapshot()
	require.NoError(t, err)

	sink := &testSnapshotSink{shortWrite: true}
	err = snapshot.Persist(sink)

	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.True(t, sink.cancelled)
	assert.False(t, sink.closed)
}

type errCloseReadCloser struct {
	io.Reader
	closeErr error
}

func (r *errCloseReadCloser) Close() error {
	return r.closeErr
}

func TestStateContainerRestoreReturnsCloseError(t *testing.T) {
	sc := newStateContainer(slog.Default())

	err := sc.Restore(&errCloseReadCloser{
		Reader:   bytes.NewBufferString(`{"state":null,"current_version":3}`),
		closeErr: errors.New("close failed"),
	})

	require.EqualError(t, err, "close failed")
}
