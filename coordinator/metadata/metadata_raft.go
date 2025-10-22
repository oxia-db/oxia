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
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/magodo/slog2hclog"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/coordinator/model"
)

type metadataProviderRaft struct {
	sync.Mutex

	sc    *stateContainer
	raft  *raft.Raft
	store *kvRaftStore
	log   *slog.Logger
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
		sc:  newStateContainer(slog.With(slog.String("component", "metadata-provider-raft-state-container"))),
		log: slog.With(slog.String("component", "metadata-provider-raft")),
	}

	// Ensure data dir per node
	nodeId := raftAddress
	dataDir := filepath.Join(raftDataDir, nodeId)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
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

	// Create stable store and log store
	mpr.store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data store")
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	// Create Raft node
	mpr.raft, err = raft.NewRaft(config, mpr.sc, mpr.store, mpr.store, snapshotStore, transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft node")
	}

	if hasState, err := raft.HasExistingState(mpr.store, mpr.store, snapshotStore); err != nil {
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
	servers := make([]raft.Server, len(bootstrapNodes))
	for i, addr := range bootstrapNodes {
		servers[i] = raft.Server{
			ID:      raft.ServerID(addr),
			Address: raft.ServerAddress(addr),
		}
	}
	return servers
}

func (mpr *metadataProviderRaft) Close() error {
	return multierr.Combine(
		mpr.raft.Shutdown().Error(),
		mpr.store.Close(),
	)
}

func toVersion(v uint64) Version {
	return Version(strconv.FormatUint(v, 10))
}

func fromVersion(v Version) uint64 {
	n, _ := strconv.ParseUint(string(v), 10, 64)
	return n
}

func (mpr *metadataProviderRaft) Get() (cs *model.ClusterStatus, version Version, err error) {
	mpr.Lock()
	defer mpr.Unlock()

	mpr.log.Debug("Get metadata",
		slog.Any("cluster-status", mpr.sc.State),
		slog.Any("current-version", mpr.sc.CurrentVersion))
	return mpr.sc.State, toVersion(mpr.sc.CurrentVersion), nil
}

func (mpr *metadataProviderRaft) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	mpr.Lock()
	defer mpr.Unlock()

	if err = mpr.raft.VerifyLeader().Error(); err != nil {
		return NotExists, err
	}

	mpr.log.Debug("Store into raft",
		slog.Any("cluster-status", cs),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", mpr.sc.CurrentVersion))

	cmd := raftOpCmd{
		NewState:        cs.Clone(),
		ExpectedVersion: fromVersion(expectedVersion),
	}

	serializedCmd, err := json.Marshal(cmd)
	if err != nil {
		return NotExists, err
	}

	future := mpr.raft.Apply(serializedCmd, 30*time.Second)
	if err := future.Error(); err != nil {
		return NotExists, errors.Wrap(err, "failed to apply metadata provider")
	}

	applyRes := future.Response().(*applyResult)
	if !applyRes.changeApplied {
		panic(ErrMetadataBadVersion)
	}

	return toVersion(applyRes.newVersion), nil
}
