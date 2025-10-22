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

package metadata

import (
	"encoding/json"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/coordinator/model"
)

type raftOpCmd struct {
	NewState        *model.ClusterStatus `json:"new_state"`
	ExpectedVersion uint64               `json:"expected_version"`
}

type stateContainer struct {
	State          *model.ClusterStatus `json:"state"`
	CurrentVersion uint64               `json:"current_version"`
	log            *slog.Logger
}

func newStateContainer(log *slog.Logger) *stateContainer {
	return &stateContainer{
		State:          &model.ClusterStatus{},
		CurrentVersion: 0,
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
			slog.Uint64("expected-version", opCmd.ExpectedVersion),
			slog.Uint64("current-version", sc.CurrentVersion),
			slog.Any("proposed-state", opCmd.NewState))

		return &applyResult{changeApplied: false}
	}

	sc.State = opCmd.NewState
	sc.CurrentVersion++

	sc.log.Info("Applied raft log entry",
		slog.Uint64("new-version", sc.CurrentVersion))
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
	sc.log.Info("Restored metadata state from snapshot",
		slog.Any("cluster-status", sc))
	return multierr.Combine(
		dec.Decode(sc),
		rc.Close(),
	)
}

func (sc *stateContainer) Persist(sink raft.SnapshotSink) error {
	value, err := json.Marshal(sc)
	if err != nil {
		return multierr.Combine(err, sink.Cancel())
	}

	_, err = sink.Write(value)
	return multierr.Combine(err, sink.Cancel(), sink.Close())
}

func (*stateContainer) Release() {}

type applyResult struct {
	changeApplied bool
	newVersion    uint64
}
