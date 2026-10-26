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

package raft

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
)

func TestStateContainerApplySupportsV0163StatusLog(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	status := &commonproto.ClusterStatus{
		InstanceId: "v0163",
		Namespaces: map[string]*commonproto.NamespaceStatus{
			"default": {},
		},
	}
	statusBytes, err := metadatacodec.ClusterStatusCodec.MarshalJSON(status)
	require.NoError(t, err)

	// v0.16.3 only stored `new_state` and `expected_version`.
	payload := map[string]any{
		"new_state":        json.RawMessage(statusBytes),
		"expected_version": int64(-1),
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	res := sc.Apply(&hashicorpraft.Log{Data: data})
	applyRes, ok := res.(*applyResult)
	require.True(t, ok)
	require.True(t, applyRes.changeApplied)
	require.Equal(t, int64(0), applyRes.newVersion)

	document := sc.document(metadatacodec.ClusterStatusCodec.GetKey())
	decoded, err := metadatacodec.ClusterStatusCodec.UnmarshalJSON(document.State)
	require.NoError(t, err)
	require.Equal(t, status.GetInstanceId(), decoded.GetInstanceId())
}

// Persist must retain the snapshot on success: a Cancel on the success path
// discards it, while the raft log still gets compacted anyway, losing the
// state on the next restart.
func TestStateContainerPersistRetainsSnapshot(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sc := newStateContainer(logger, nil)

	status := &commonproto.ClusterStatus{InstanceId: "snapshot-test"}
	statusBytes, err := metadatacodec.ClusterStatusCodec.MarshalJSON(status)
	require.NoError(t, err)

	cmd, err := json.Marshal(raftOpCmd{
		Key:             metadatacodec.ClusterStatusCodec.GetKey(),
		NewState:        statusBytes,
		ExpectedVersion: -1,
	})
	require.NoError(t, err)
	res := sc.Apply(&hashicorpraft.Log{Data: cmd})
	require.True(t, res.(*applyResult).changeApplied)

	snapshot, err := sc.Snapshot()
	require.NoError(t, err)

	store, err := hashicorpraft.NewFileSnapshotStoreWithLogger(t.TempDir(), 2, nil)
	require.NoError(t, err)
	_, trans := hashicorpraft.NewInmemTransport("")
	sink, err := store.Create(hashicorpraft.SnapshotVersionMax, 1, 1, hashicorpraft.Configuration{}, 0, trans)
	require.NoError(t, err)

	require.NoError(t, snapshot.Persist(sink))
	// Raft releases the snapshot once it is persisted
	snapshot.Release()

	// The snapshot must be retained, not canceled
	snapshots, err := store.List()
	require.NoError(t, err)
	require.Len(t, snapshots, 1)

	// And a fresh container must restore the state from it
	_, rc, err := store.Open(snapshots[0].ID)
	require.NoError(t, err)

	restored := newStateContainer(logger, nil)
	require.NoError(t, restored.Restore(rc))

	doc := restored.document(metadatacodec.ClusterStatusCodec.GetKey())
	require.EqualValues(t, 0, doc.CurrentVersion)
	restoredStatus, err := metadatacodec.ClusterStatusCodec.UnmarshalJSON(doc.State)
	require.NoError(t, err)
	require.Equal(t, "snapshot-test", restoredStatus.InstanceId)
}

func TestStateContainerApplyRejectsUndecodableEntry(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	res, ok := sc.Apply(&hashicorpraft.Log{Data: []byte("not json")}).(*applyResult)
	require.True(t, ok)
	assert.False(t, res.changeApplied)
	assert.Empty(t, sc.Documents)
}

func TestStateContainerRestoreRejectsUndecodableSnapshot(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	rc := &fakeReadCloser{Reader: strings.NewReader("not json")}

	require.Error(t, sc.Restore(rc))
	assert.True(t, rc.closed)
}

// A snapshot taken before the container held several documents only has the
// status state: it is restored as the status document.
func TestStateContainerRestoreLegacySnapshot(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	rc := &fakeReadCloser{Reader: strings.NewReader(`{"state":{"instanceId":"legacy"},"current_version":3}`)}

	require.NoError(t, sc.Restore(rc))
	assert.True(t, rc.closed)
	state, version := sc.documentState(metadatacodec.ClusterStatusCodec.GetKey())
	assert.EqualValues(t, 3, version)
	status, err := metadatacodec.ClusterStatusCodec.UnmarshalJSON(state)
	require.NoError(t, err)
	assert.Equal(t, "legacy", status.InstanceId)
}

func TestStateContainerRestoreEmptyLegacySnapshot(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	require.NoError(t, sc.Restore(&fakeReadCloser{Reader: strings.NewReader(`{}`)}))
	state, version := sc.documentState(metadatacodec.ClusterStatusCodec.GetKey())
	assert.Nil(t, state)
	assert.EqualValues(t, 0, version)
}

func TestStateContainerPersistCancelsOnFailure(t *testing.T) {
	errSink := errors.New("sink failure")
	for _, tc := range []struct {
		name      string
		documents map[string]*documentContainer
		sink      *fakeSnapshotSink
	}{
		{
			name:      "unmarshalable state",
			documents: map[string]*documentContainer{"key": {State: json.RawMessage("{")}},
			sink:      &fakeSnapshotSink{},
		},
		{name: "write failure", sink: &fakeSnapshotSink{writeErr: errSink}},
		{name: "close failure", sink: &fakeSnapshotSink{closeErr: errSink}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if tc.documents != nil {
				sc.Documents = tc.documents
			}

			require.Error(t, sc.Persist(tc.sink))
			assert.True(t, tc.sink.canceled)
		})
	}
}

type fakeReadCloser struct {
	io.Reader
	closed bool
}

func (r *fakeReadCloser) Close() error {
	r.closed = true
	return nil
}

type fakeSnapshotSink struct {
	writeErr error
	closeErr error
	canceled bool
}

func (*fakeSnapshotSink) ID() string {
	return "fake"
}

func (s *fakeSnapshotSink) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return len(p), nil
}

func (s *fakeSnapshotSink) Close() error {
	return s.closeErr
}

func (s *fakeSnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}
