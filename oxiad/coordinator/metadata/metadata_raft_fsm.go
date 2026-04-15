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
	"sync"

	"github.com/hashicorp/raft"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type raftOpCmd struct {
	NewState        *model.ClusterStatus `json:"new_state"`
	ExpectedVersion int64                `json:"expected_version"`
}

type stateContainer struct {
	sync.RWMutex `json:"-"`

	State          *model.ClusterStatus `json:"state"`
	CurrentVersion int64                `json:"current_version"`
	log            *slog.Logger         `json:"-"`
}

type stateSnapshot struct {
	State          *model.ClusterStatus `json:"state"`
	CurrentVersion int64                `json:"current_version"`
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
	opCmd := raftOpCmd{}
	err := json.Unmarshal(logEntry.Data, &opCmd)
	if err != nil {
		sc.log.Error("failed to deserialize state",
			slog.Any("error", err))
		return &applyResult{changeApplied: false}
	}

	sc.Lock()
	defer sc.Unlock()

	if opCmd.ExpectedVersion != sc.CurrentVersion {
		sc.log.Warn("Failed to apply raft state",
			slog.Int64("expected-version", opCmd.ExpectedVersion),
			slog.Int64("current-version", sc.CurrentVersion),
			slog.Any("proposed-state", opCmd.NewState))

		return &applyResult{changeApplied: false}
	}

	if opCmd.NewState == nil {
		sc.State = nil
	} else {
		sc.State = opCmd.NewState.Clone()
	}
	sc.CurrentVersion++

	sc.log.Info("Applied raft log entry",
		slog.Int64("new-version", sc.CurrentVersion))
	return &applyResult{changeApplied: true, newVersion: sc.CurrentVersion}
}

// Snapshot returns a snapshot of the FSM.
func (sc *stateContainer) Snapshot() (raft.FSMSnapshot, error) {
	state, version := sc.CloneState()
	return &stateContainer{
		State:          state,
		CurrentVersion: version,
	}, nil
}

// Restore stores the key-value pairs from a snapshot.
func (sc *stateContainer) Restore(rc io.ReadCloser) (err error) {
	defer func() {
		err = multierr.Append(err, rc.Close())
	}()

	dec := json.NewDecoder(rc)
	snapshot := stateSnapshot{}
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}

	sc.Lock()
	defer sc.Unlock()

	sc.State = snapshot.State
	sc.CurrentVersion = snapshot.CurrentVersion
	sc.log.Info("Restored metadata state from snapshot",
		slog.Any("cluster-status", sc))
	return nil
}

func (sc *stateContainer) Persist(sink raft.SnapshotSink) error {
	state, version := sc.CloneState()
	value, err := json.Marshal(stateSnapshot{
		State:          state,
		CurrentVersion: version,
	})
	if err != nil {
		return multierr.Combine(err, sink.Cancel())
	}

	n, err := sink.Write(value)
	if err != nil {
		return multierr.Combine(err, sink.Cancel())
	}
	if n != len(value) {
		return multierr.Combine(io.ErrShortWrite, sink.Cancel())
	}
	return sink.Close()
}

func (sc *stateContainer) CloneState() (*model.ClusterStatus, int64) {
	sc.RLock()
	defer sc.RUnlock()

	if sc.State == nil {
		return nil, sc.CurrentVersion
	}
	return sc.State.Clone(), sc.CurrentVersion
}

func (*stateContainer) Release() {}

type applyResult struct {
	changeApplied bool
	newVersion    int64
}
