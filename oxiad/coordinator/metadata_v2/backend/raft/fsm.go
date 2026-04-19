package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/backend"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

type raftCommand struct {
	Name            string `json:"name"`
	ExpectedVersion string `json:"expected_version,omitempty"`
	Value           []byte `json:"value,omitempty"`
}

type recordState struct {
	Version uint64 `json:"version"`
	Value   []byte `json:"value,omitempty"`
}

type stateSnapshot struct {
	Records map[string]recordState `json:"records"`
}

type storeResult struct {
	Version string `json:"version"`
}

type stateContainer struct {
	mu      sync.RWMutex
	records map[string]recordState
}

func newStateContainer() *stateContainer {
	return &stateContainer{
		records: make(map[string]recordState),
	}
}

func (s *stateContainer) Apply(logEntry *hashicorpraft.Log) any {
	cmd, err := decodeRaftCommand(logEntry.Data)
	if err != nil {
		return err
	}

	switch backend.MetaRecordName(cmd.Name) {
	case backend.ConfigRecordName, backend.StatusRecordName:
	default:
		return fmt.Errorf("unknown metadata record %q", cmd.Name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.records[cmd.Name]
	if cmd.ExpectedVersion != formatVersion(current.Version) {
		return document.ErrBadVersion
	}

	next := recordState{
		Version: current.Version + 1,
		Value:   cloneBytes(cmd.Value),
	}
	s.records[cmd.Name] = next
	return storeResult{
		Version: formatVersion(next.Version),
	}
}

func (s *stateContainer) Snapshot() (hashicorpraft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	records := make(map[string]recordState, len(s.records))
	for name, record := range s.records {
		records[name] = recordState{
			Version: record.Version,
			Value:   cloneBytes(record.Value),
		}
	}
	return &stateSnapshot{Records: records}, nil
}

func (s *stateContainer) Restore(rc io.ReadCloser) error {
	defer func() {
		_ = rc.Close()
	}()

	snapshot := &stateSnapshot{}
	if err := json.NewDecoder(rc).Decode(snapshot); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = make(map[string]recordState, len(snapshot.Records))
	for name, record := range snapshot.Records {
		s.records[name] = recordState{
			Version: record.Version,
			Value:   cloneBytes(record.Value),
		}
	}
	return nil
}

func (s *stateContainer) Load(name backend.MetaRecordName) (recordState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.records[string(name)]
	if !ok {
		return recordState{}, false
	}

	return recordState{
		Version: record.Version,
		Value:   cloneBytes(record.Value),
	}, true
}

func (s *stateSnapshot) Persist(sink hashicorpraft.SnapshotSink) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return multierr.Combine(err, sink.Cancel())
	}
	if _, err := sink.Write(payload); err != nil {
		return multierr.Combine(err, sink.Cancel())
	}
	return sink.Close()
}

func (*stateSnapshot) Release() {}

func encodeRaftCommand(cmd raftCommand) []byte {
	payload, err := json.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	return payload
}

func decodeRaftCommand(payload []byte) (raftCommand, error) {
	cmd := raftCommand{}
	return cmd, json.Unmarshal(payload, &cmd)
}

func cloneBytes(data []byte) []byte {
	return append([]byte(nil), data...)
}

func formatVersion(version uint64) string {
	if version == 0 {
		return ""
	}
	return strconv.FormatUint(version, 10)
}
