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
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/magodo/slog2hclog"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type Raft struct {
	mu          sync.Mutex
	sc          *stateContainer
	node        *hashicorpraft.Raft
	store       *kvRaftStore
	logger      *slog.Logger
	interceptor Interceptor

	closeOnce sync.Once
	closeErr  error
}

const raftDataDirMode = 0o755

func New(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
	interceptor Interceptor,
) (*Raft, error) {
	metadataRaft := &Raft{
		logger:      slog.With(slog.String("component", "metadata-provider-raft")),
		interceptor: interceptor,
	}
	metadataRaft.sc = newStateContainer(
		slog.With(slog.String("component", "metadata-provider-raft-state-container")),
		metadataRaft.interceptor,
	)

	nodeID := raftAddress
	dataDir := filepath.Join(raftDataDir, nodeID)
	if err := os.MkdirAll(dataDir, raftDataDirMode); err != nil {
		return nil, errors.Wrap(err, "failed to create data dir")
	}

	config := hashicorpraft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LocalID = hashicorpraft.ServerID(nodeID)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(metadataRaft.logger, levelVar)

	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve raft address")
	}
	transport, err := hashicorpraft.NewTCPTransport(raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft transport")
	}

	metadataRaft.store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data store")
	}

	snapshotStore, err := hashicorpraft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	metadataRaft.node, err = hashicorpraft.NewRaft(config, metadataRaft.sc, metadataRaft.store, metadataRaft.store, snapshotStore, transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft node")
	}

	if hasState, err := hashicorpraft.HasExistingState(metadataRaft.store, metadataRaft.store, snapshotStore); err != nil {
		return nil, errors.Wrap(err, "failed to check existing state")
	} else if !hasState {
		configuration := hashicorpraft.Configuration{
			Servers: getRaftServers(raftBootstrapNodes),
		}
		future := metadataRaft.node.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return nil, errors.Wrap(err, "failed to create raft node")
		}
	}

	return metadataRaft, nil
}

func getRaftServers(bootstrapNodes []string) []hashicorpraft.Server {
	servers := make([]hashicorpraft.Server, len(bootstrapNodes))
	for i, addr := range bootstrapNodes {
		servers[i] = hashicorpraft.Server{
			ID:      hashicorpraft.ServerID(addr),
			Address: hashicorpraft.ServerAddress(addr),
		}
	}
	return servers
}

func (r *Raft) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = multierr.Combine(
			r.node.Shutdown().Error(),
			r.store.Close(),
		)
	})
	return r.closeErr
}

var _ io.Closer = (*Raft)(nil)
