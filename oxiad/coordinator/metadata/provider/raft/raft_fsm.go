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

package raft

import (
	"encoding/json"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"
	"go.uber.org/multierr"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
)

type raftOpCmd struct {
	Key             string          `json:"key"`
	NewState        json.RawMessage `json:"new_state"`
	ExpectedVersion int64           `json:"expected_version"`
}

type stateContainer struct {
	Documents   map[string]*documentContainer `json:"-"`
	logger      *slog.Logger
	interceptor Interceptor
}

func newStateContainer(logger *slog.Logger, interceptor Interceptor) *stateContainer {
	return &stateContainer{
		Documents:   map[string]*documentContainer{},
		logger:      logger,
		interceptor: interceptor,
	}
}

// Apply applies a Raft log entry to the FSM.
func (sc *stateContainer) Apply(logEntry *raft.Log) any {
	// Each log entry contains the whole state
	opCmd := raftOpCmd{}
	err := json.Unmarshal(logEntry.Data, &opCmd)
	if err != nil {
		sc.logger.Error("failed to deserialize state",
			slog.Any("error", err))
		return &applyResult{changeApplied: false}
	}
	if opCmd.Key == "" {
		opCmd.Key = metadatacommon.ClusterStatusCodec.GetKey()
	}

	document := sc.document(opCmd.Key)
	if opCmd.ExpectedVersion != document.CurrentVersion {
		sc.logger.Warn("Failed to apply raft state",
			slog.String("key", opCmd.Key),
			slog.Int64("expected-version", opCmd.ExpectedVersion),
			slog.Int64("current-version", document.CurrentVersion),
			slog.Any("proposed-state", opCmd.NewState))

		return &applyResult{changeApplied: false}
	}

	document.State = cloneBytes(opCmd.NewState)
	document.CurrentVersion++
	if sc.interceptor != nil {
		sc.interceptor.OnApplied(opCmd.Key, document.State, document.CurrentVersion)
	}

	sc.logger.Info("Applied raft log entry",
		slog.String("key", opCmd.Key),
		slog.Int64("new-version", document.CurrentVersion))
	return &applyResult{changeApplied: true, newVersion: document.CurrentVersion}
}

// Snapshot returns a snapshot of the FSM.
func (sc *stateContainer) Snapshot() (raft.FSMSnapshot, error) {
	return &stateContainer{
		Documents: cloneDocuments(sc.Documents),
	}, nil
}

// Restore stores the key-value pairs from a snapshot.
func (sc *stateContainer) Restore(rc io.ReadCloser) error {
	dec := json.NewDecoder(rc)
	persisted := &persistedStateContainer{}
	sc.logger.Info("Restored metadata state from snapshot",
		slog.Any("cluster-status", sc))

	if err := dec.Decode(persisted); err != nil {
		return multierr.Combine(err, rc.Close())
	}

	if len(persisted.Documents) > 0 {
		sc.Documents = cloneDocuments(persisted.Documents)
		return rc.Close()
	}

	sc.Documents = map[string]*documentContainer{
		metadatacommon.ClusterStatusCodec.GetKey(): {
			State:          cloneBytes(persisted.State),
			CurrentVersion: persisted.CurrentVersion,
		},
	}
	return rc.Close()
}

func (sc *stateContainer) Persist(sink raft.SnapshotSink) error {
	payload, err := marshalStateContainer(sc)
	if err != nil {
		return multierr.Combine(err, sink.Cancel())
	}

	_, err = sink.Write(payload)
	return multierr.Combine(err, sink.Cancel(), sink.Close())
}

func (*stateContainer) Release() {}

type applyResult struct {
	changeApplied bool
	newVersion    int64
}

type persistedStateContainer struct {
	Documents      map[string]*documentContainer `json:"documents,omitempty"`
	State          json.RawMessage               `json:"state,omitempty"`
	CurrentVersion int64                         `json:"current_version,omitempty"`
}

type documentContainer struct {
	State          json.RawMessage `json:"state"`
	CurrentVersion int64           `json:"current_version"`
}

func marshalStateContainer(sc *stateContainer) ([]byte, error) {
	return json.Marshal(persistedStateContainer{
		Documents: cloneDocuments(sc.Documents),
	})
}

func (sc *stateContainer) document(key string) *documentContainer {
	document, ok := sc.Documents[key]
	if !ok {
		document = &documentContainer{CurrentVersion: -1}
		sc.Documents[key] = document
	}
	return document
}

func cloneDocuments(documents map[string]*documentContainer) map[string]*documentContainer {
	cloned := make(map[string]*documentContainer, len(documents))
	for key, document := range documents {
		cloned[key] = &documentContainer{
			State:          cloneBytes(document.State),
			CurrentVersion: document.CurrentVersion,
		}
	}
	return cloned
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
