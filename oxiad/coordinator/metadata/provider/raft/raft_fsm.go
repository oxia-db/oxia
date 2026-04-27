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

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type raftOpCmd struct {
	ResourceType    provider.ResourceType `json:"resource_type"`
	NewState        json.RawMessage       `json:"new_state"`
	ExpectedVersion int64                 `json:"expected_version"`
}

type stateContainer struct {
	Documents map[provider.ResourceType]*documentContainer `json:"-"`
	log       *slog.Logger
}

func newStateContainer(log *slog.Logger) *stateContainer {
	return &stateContainer{
		Documents: map[provider.ResourceType]*documentContainer{},
		log:       log,
	}
}

// Apply applies a Raft log entry to the FSM.
func (sc *stateContainer) Apply(logEntry *raft.Log) any {
	// Each log entry contains the whole state
	opCmd := raftOpCmd{}
	err := json.Unmarshal(logEntry.Data, &opCmd)
	if err != nil {
		sc.log.Error("failed to deserialize state",
			slog.Any("error", err))
		return &applyResult{changeApplied: false}
	}
	if opCmd.ResourceType == "" {
		opCmd.ResourceType = provider.ResourceStatus
	}

	document := sc.document(opCmd.ResourceType)
	if opCmd.ExpectedVersion != document.CurrentVersion {
		sc.log.Warn("Failed to apply raft state",
			slog.String("resource-type", string(opCmd.ResourceType)),
			slog.Int64("expected-version", opCmd.ExpectedVersion),
			slog.Int64("current-version", document.CurrentVersion),
			slog.Any("proposed-state", opCmd.NewState))

		return &applyResult{changeApplied: false}
	}

	document.State = cloneBytes(opCmd.NewState)
	document.CurrentVersion++

	sc.log.Info("Applied raft log entry",
		slog.String("resource-type", string(opCmd.ResourceType)),
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
	sc.log.Info("Restored metadata state from snapshot",
		slog.Any("cluster-status", sc))

	if err := dec.Decode(persisted); err != nil {
		return multierr.Combine(err, rc.Close())
	}

	if len(persisted.Documents) > 0 {
		sc.Documents = cloneDocuments(persisted.Documents)
		return rc.Close()
	}

	sc.Documents = map[provider.ResourceType]*documentContainer{
		provider.ResourceStatus: {
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
	Documents      map[provider.ResourceType]*documentContainer `json:"documents,omitempty"`
	State          json.RawMessage                              `json:"state,omitempty"`
	CurrentVersion int64                                        `json:"current_version,omitempty"`
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

func (sc *stateContainer) document(resourceType provider.ResourceType) *documentContainer {
	document, ok := sc.Documents[resourceType]
	if !ok {
		document = &documentContainer{CurrentVersion: -1}
		sc.Documents[resourceType] = document
	}
	return document
}

func cloneDocuments(documents map[provider.ResourceType]*documentContainer) map[provider.ResourceType]*documentContainer {
	cloned := make(map[provider.ResourceType]*documentContainer, len(documents))
	for resourceType, document := range documents {
		cloned[resourceType] = &documentContainer{
			State:          cloneBytes(document.State),
			CurrentVersion: document.CurrentVersion,
		}
	}
	return cloned
}
