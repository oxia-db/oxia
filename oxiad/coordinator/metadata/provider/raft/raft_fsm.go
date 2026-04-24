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

	commonproto "github.com/oxia-db/oxia/common/proto"
)

type raftOpCmd struct {
	NewState        json.RawMessage `json:"new_state"`
	ExpectedVersion int64           `json:"expected_version"`
}

type stateContainer struct {
	State          *commonproto.ClusterStatus `json:"-"`
	CurrentVersion int64                      `json:"current_version"`
	log            *slog.Logger
}

func newStateContainer(log *slog.Logger) *stateContainer {
	return &stateContainer{
		State:          nil,
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

	newState, err := commonproto.UnmarshalClusterStatusJSON(opCmd.NewState)
	if err != nil {
		sc.log.Error("failed to deserialize cluster status",
			slog.Any("error", err))
		return &applyResult{changeApplied: false}
	}

	sc.State = newState
	sc.CurrentVersion++

	sc.log.Info("Applied raft log entry",
		slog.Int64("new-version", sc.CurrentVersion))
	return &applyResult{changeApplied: true, newVersion: sc.CurrentVersion}
}

// Snapshot returns a snapshot of the FSM.
func (sc *stateContainer) Snapshot() (raft.FSMSnapshot, error) {
	return &stateContainer{
		State:          sc.State.Clone(),
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

	state, err := commonproto.UnmarshalClusterStatusJSON(persisted.State)
	if err != nil {
		return multierr.Combine(err, rc.Close())
	}

	sc.State = state
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
	State          json.RawMessage `json:"state"`
	CurrentVersion int64           `json:"current_version"`
}

func marshalStateContainer(sc *stateContainer) ([]byte, error) {
	stateBytes, err := commonproto.MarshalClusterStatusJSON(sc.State)
	if err != nil {
		return nil, err
	}

	return json.Marshal(persistedStateContainer{
		State:          stateBytes,
		CurrentVersion: sc.CurrentVersion,
	})
}

func mustMarshalClusterStatus(status *commonproto.ClusterStatus) json.RawMessage {
	payload, err := commonproto.MarshalClusterStatusJSON(status)
	if err != nil {
		panic(err)
	}
	return payload
}
