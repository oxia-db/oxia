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
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type raftOpCmd struct {
	Document        provider.Document `json:"document,omitempty"`
	NewState        json.RawMessage   `json:"new_state"`
	ExpectedVersion int64             `json:"expected_version"`
}

type stateContainer struct {
	State          *commonproto.ClusterStatus `json:"-"`
	Documents      map[provider.Document]json.RawMessage
	CurrentVersion int64 `json:"current_version"`
	log            *slog.Logger
}

func newStateContainer(log *slog.Logger) *stateContainer {
	return &stateContainer{
		State:          nil,
		Documents:      map[provider.Document]json.RawMessage{},
		CurrentVersion: -1,
		log:            log,
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

	if opCmd.ExpectedVersion != sc.CurrentVersion {
		sc.log.Warn("Failed to apply raft state",
			slog.Int64("expected-version", opCmd.ExpectedVersion),
			slog.Int64("current-version", sc.CurrentVersion),
			slog.Any("proposed-state", opCmd.NewState))

		return &applyResult{changeApplied: false}
	}

	document := opCmd.Document
	if document == "" {
		document = provider.DocumentClusterStatus
	}

	if err := sc.applyDocument(document, opCmd.NewState); err != nil {
		sc.log.Error("failed to apply raft metadata document",
			slog.String("document", string(document)),
			slog.Any("error", err))
		return &applyResult{changeApplied: false}
	}

	sc.CurrentVersion++

	sc.log.Info("Applied raft log entry",
		slog.Int64("new-version", sc.CurrentVersion))
	return &applyResult{changeApplied: true, newVersion: sc.CurrentVersion}
}

func (sc *stateContainer) applyDocument(document provider.Document, data json.RawMessage) error {
	switch document {
	case provider.DocumentClusterStatus:
		newState, err := commonproto.UnmarshalClusterStatusJSON(data)
		if err != nil {
			return err
		}
		sc.State = newState
	case provider.DocumentClusterConfiguration:
		if sc.Documents == nil {
			sc.Documents = map[provider.Document]json.RawMessage{}
		}
		sc.Documents[document] = append(json.RawMessage(nil), data...)
	default:
		return errors.Errorf("unsupported metadata document %q", document)
	}
	return nil
}

// Snapshot returns a snapshot of the FSM.
func (sc *stateContainer) Snapshot() (raft.FSMSnapshot, error) {
	var state *commonproto.ClusterStatus
	if sc.State != nil {
		state = gproto.Clone(sc.State).(*commonproto.ClusterStatus) //nolint:revive
	}
	return &stateContainer{
		State:          state,
		Documents:      cloneDocuments(sc.Documents),
		CurrentVersion: sc.CurrentVersion,
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

	if len(persisted.State) > 0 {
		state, err := commonproto.UnmarshalClusterStatusJSON(persisted.State)
		if err != nil {
			return multierr.Combine(err, rc.Close())
		}
		sc.State = state
	}

	sc.Documents = cloneDocuments(persisted.Documents)
	sc.CurrentVersion = persisted.CurrentVersion
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
	State          json.RawMessage                       `json:"state"`
	Documents      map[provider.Document]json.RawMessage `json:"documents,omitempty"`
	CurrentVersion int64                                 `json:"current_version"`
}

func marshalStateContainer(sc *stateContainer) ([]byte, error) {
	var stateBytes json.RawMessage
	if sc.State != nil {
		var err error
		stateBytes, err = commonproto.MarshalClusterStatusJSON(sc.State)
		if err != nil {
			return nil, err
		}
	}

	return json.Marshal(persistedStateContainer{
		State:          stateBytes,
		Documents:      cloneDocuments(sc.Documents),
		CurrentVersion: sc.CurrentVersion,
	})
}

func cloneDocuments(documents map[provider.Document]json.RawMessage) map[provider.Document]json.RawMessage {
	if len(documents) == 0 {
		return nil
	}
	cloned := make(map[provider.Document]json.RawMessage, len(documents))
	for document, data := range documents {
		cloned[document] = append(json.RawMessage(nil), data...)
	}
	return cloned
}
