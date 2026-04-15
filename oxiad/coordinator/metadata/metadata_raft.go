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

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type metadataProviderRaft struct {
	sc    *stateContainer
	raft  *raft.Raft
	store *kvRaftStore

	transport              *raft.NetworkTransport
	leadershipLostCh       chan struct{}
	leadershipNotifyCh     chan bool
	leadershipNotifyStopCh chan struct{}
	closeOnce              sync.Once
	log                    *slog.Logger
}

func (mpr *metadataProviderRaft) WaitToBecomeLeader() error {
	if err := mpr.waitForLeaderState(); err != nil {
		return err
	}
	return mpr.waitForAppliedState()
}

func NewMetadataProviderRaft(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (Provider, error) {
	mpr := &metadataProviderRaft{
		sc:                     newStateContainer(slog.With(slog.String("component", "metadata-provider-raft-state-container"))),
		leadershipLostCh:       make(chan struct{}, 1),
		leadershipNotifyCh:     make(chan bool, 8),
		leadershipNotifyStopCh: make(chan struct{}),
		log:                    slog.With(slog.String("component", "metadata-provider-raft")),
	}

	// Ensure data dir per node
	nodeID := raftAddress
	dataDir := filepath.Join(raftDataDir, nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create data dir")
	}

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LocalID = raft.ServerID(nodeID)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(mpr.log, levelVar)
	config.NotifyCh = mpr.leadershipNotifyCh

	var raftNode *raft.Raft
	var store *kvRaftStore
	var transport *raft.NetworkTransport
	var snapshotStore *raft.FileSnapshotStore
	cleanup := true
	defer func() {
		if !cleanup {
			return
		}
		if raftNode != nil {
			_ = raftNode.Shutdown().Error()
		}
		if transport != nil {
			_ = transport.Close()
		}
		if store != nil {
			_ = store.Close()
		}
	}()

	// Create TCP transport for Raft
	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve raft address")
	}
	transport, err = raft.NewTCPTransportWithLogger(raftAddress, addr, 3, 10*time.Second, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft transport")
	}

	// Create stable store and log store
	store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data store")
	}

	// Create snapshot store
	snapshotStore, err = raft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	// Create Raft node
	raftNode, err = raft.NewRaft(config, mpr.sc, store, store, snapshotStore, transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft node")
	}

	if hasState, err := raft.HasExistingState(store, store, snapshotStore); err != nil {
		return nil, errors.Wrap(err, "failed to check existing state")
	} else if !hasState {
		configuration := raft.Configuration{
			Servers: getRaftServers(raftBootstrapNodes),
		}
		future := raftNode.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return nil, errors.Wrap(err, "failed to bootstrap raft cluster")
		}
	}

	mpr.raft = raftNode
	mpr.store = store
	mpr.transport = transport
	go mpr.watchLeadershipChanges(mpr.raft.State() == raft.Leader)
	cleanup = false

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
	var err error
	mpr.closeOnce.Do(func() {
		err = multierr.Combine(
			mpr.raft.Shutdown().Error(),
			mpr.transport.Close(),
			mpr.store.Close(),
		)
		close(mpr.leadershipNotifyStopCh)
	})
	return err
}

func toVersion(v int64) Version {
	return Version(strconv.FormatInt(v, 10))
}

func fromVersion(v Version) int64 {
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		panic(ErrMetadataBadVersion)
	}
	return n
}

func (mpr *metadataProviderRaft) Get() (cs *model.ClusterStatus, version Version, err error) {
	if err = mpr.raft.VerifyLeader().Error(); err != nil {
		return nil, NotExists, err
	}
	if err = mpr.waitForAppliedState(); err != nil {
		return nil, NotExists, err
	}

	state, currentVersion := mpr.sc.CloneState()

	mpr.log.Debug("Get metadata",
		slog.Any("cluster-status", state),
		slog.Any("current-version", currentVersion))
	return state, toVersion(currentVersion), nil
}

func (mpr *metadataProviderRaft) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	if err = mpr.raft.VerifyLeader().Error(); err != nil {
		return NotExists, err
	}

	_, currentVersion := mpr.sc.CloneState()
	mpr.log.Debug("Store into raft",
		slog.Any("cluster-status", cs),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", currentVersion))

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
		return NotExists, errors.Wrap(err, "failed to apply new cluster state")
	}

	applyRes, ok := future.Response().(*applyResult)
	if !ok {
		return NotExists, errors.Errorf("unexpected raft apply response type %T", future.Response())
	}

	if !applyRes.changeApplied {
		panic(ErrMetadataBadVersion)
	}

	return toVersion(applyRes.newVersion), nil
}

func (mpr *metadataProviderRaft) LeadershipLost() <-chan struct{} {
	return mpr.leadershipLostCh
}

func (mpr *metadataProviderRaft) waitForLeaderState() error {
	if mpr.raft.State() == raft.Leader {
		return nil
	}

	leaderCh := mpr.raft.LeaderCh()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case isLeader, ok := <-leaderCh:
			if !ok {
				return raft.ErrRaftShutdown
			}
			if isLeader {
				return nil
			}
		case <-ticker.C:
			switch mpr.raft.State() {
			case raft.Leader:
				return nil
			case raft.Shutdown:
				return raft.ErrRaftShutdown
			}
		}
	}
}

func (mpr *metadataProviderRaft) waitForAppliedState() error {
	return mpr.raft.Barrier(30 * time.Second).Error()
}

func (mpr *metadataProviderRaft) watchLeadershipChanges(wasLeader bool) {
	for {
		select {
		case <-mpr.leadershipNotifyStopCh:
			return
		case isLeader := <-mpr.leadershipNotifyCh:
			if wasLeader && !isLeader {
				select {
				case mpr.leadershipLostCh <- struct{}{}:
				default:
				}
			}
			wasLeader = isLeader
		}
	}
}
