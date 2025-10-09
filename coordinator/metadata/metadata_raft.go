// Copyright 2023 StreamNative, Inc.
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
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/magodo/slog2hclog"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/pkg/errors"
)

var _ Provider = &metadataProviderRaft{}

type stateContainer struct {
	ClusterStatus *model.ClusterStatus `json:"clusterStatus"`
	Version       Version              `json:"version"`
}

func newStateContainer() *stateContainer {
	return &stateContainer{
		ClusterStatus: nil,
		Version:       NotExists,
	}
}

// Apply applies a Raft log entry to the FSM.
func (sc *stateContainer) Apply(logEntry *raft.Log) interface{} {
	// Each log entry contains the whole state
	return json.Unmarshal(logEntry.Data, sc)
}

// Snapshot returns a snapshot of the FSM.
func (sc *stateContainer) Snapshot() (raft.FSMSnapshot, error) {
	return &stateSnapshot{
		ClusterStatus: sc.ClusterStatus.Clone(),
		Version:       sc.Version,
	}, nil
}

// Restore stores the key-value pairs from a snapshot.
func (sc *stateContainer) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	dec := json.NewDecoder(rc)
	return dec.Decode(sc)
}

type stateSnapshot struct {
	ClusterStatus *model.ClusterStatus `json:"clusterStatus"`
	Version       Version              `json:"version"`
}

func (s *stateSnapshot) Persist(sink raft.SnapshotSink) error {
	value, err := json.Marshal(s)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(value); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		return err
	}
	return nil
}

func (s *stateSnapshot) Release() {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type metadataProviderRaft struct {
	sync.Mutex

	sc   *stateContainer
	raft *raft.Raft
	log  *slog.Logger
}

func (mpr *metadataProviderRaft) WaitToBecomeLeader() error {
	<-mpr.raft.LeaderCh()
	return nil
}

func NewMetadataProviderRaft(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (Provider, error) {
	mpr := &metadataProviderRaft{
		sc:  newStateContainer(),
		log: slog.With(slog.String("component", "metadata-provider-raft")),
	}

	// Ensure data dir per node
	nodeId := raftAddress
	dataDir := filepath.Join(raftDataDir, nodeId)
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, errors.Wrap(err, "failed to create data dir")
	}

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LocalID = raft.ServerID(nodeId)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(mpr.log, levelVar)

	// Create TCP transport for Raft
	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve raft address")
	}
	transport, err := raft.NewTCPTransport(raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft transport")
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	// Create BoltDB-based stable store and log store
	boltDBPath := filepath.Join(dataDir, "raft.db")
	store, err := boltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bolt store")
	}

	// Create Raft node
	mpr.raft, err = raft.NewRaft(config, mpr.sc, store, store, snapshotStore, transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft node")
	}

	if hasState, err := raft.HasExistingState(store, store, snapshotStore); err != nil {
		return nil, errors.Wrap(err, "failed to check existing state")
	} else if !hasState {
		configuration := raft.Configuration{
			Servers: getRaftServers(raftBootstrapNodes),
		}
		future := mpr.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return nil, errors.Wrap(err, "failed to create raft node")
		}
	}

	return mpr, nil
}

func getRaftServers(bootstrapNodes []string) []raft.Server {
	var servers []raft.Server
	for _, addr := range bootstrapNodes {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(addr),
			Address: raft.ServerAddress(addr),
		})
	}
	return servers
}

func (mpr *metadataProviderRaft) Close() error {
	return nil
}

func (mpr *metadataProviderRaft) Get() (cs *model.ClusterStatus, version Version, err error) {
	mpr.Lock()
	defer mpr.Unlock()

	return mpr.sc.ClusterStatus, mpr.sc.Version, nil
}

func (mpr *metadataProviderRaft) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	mpr.Lock()
	defer mpr.Unlock()

	if err = mpr.raft.VerifyLeader().Error(); err != nil {
		return NotExists, err
	}

	if mpr.sc.Version != expectedVersion {
		panic(ErrMetadataBadVersion)
	}

	newState := &stateContainer{
		ClusterStatus: cs.Clone(),
		Version:       incrVersion(mpr.sc.Version),
	}

	serialized, err := json.Marshal(newState)
	if err != nil {
		return NotExists, err
	}

	future := mpr.raft.Apply(serialized, 30*time.Second)
	if err := future.Error(); err != nil {
		return NotExists, errors.Wrap(err, "failed to apply metadata provider")
	}

	mpr.sc = newState
	return newState.Version, nil
}
