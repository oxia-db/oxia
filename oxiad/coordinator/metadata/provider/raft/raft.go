// Copyright 2023-2026 The Oxia Authors
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
	transport   *hashicorpraft.NetworkTransport
	logger      *slog.Logger
	interceptor Interceptor

	// Raft blocks writing leadership transitions to notifyCh: it must always
	// have a consumer (waitToBecomeLeader, then the loss drainer)
	notifyCh   chan bool
	shutdownCh chan struct{}

	closeOnce sync.Once
	closeErr  error
}

const raftDataDirMode = 0o755

func New(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
	interceptor Interceptor,
) (_ *Raft, err error) {
	metadataRaft := &Raft{
		logger:      slog.With(slog.String("component", "metadata-provider-raft")),
		interceptor: interceptor,
		notifyCh:    make(chan bool, 1),
		shutdownCh:  make(chan struct{}),
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
	config.NotifyCh = metadataRaft.notifyCh
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(metadataRaft.logger, levelVar)

	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve raft address")
	}
	metadataRaft.transport, err = hashicorpraft.NewTCPTransport(raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft transport")
	}

	// Release whatever has been created so far when a later step fails:
	// the transport holds a TCP listener and the store an open database
	defer func() {
		if err != nil {
			_ = metadataRaft.release()
		}
	}()

	metadataRaft.store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data store")
	}

	snapshotStore, err := hashicorpraft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	metadataRaft.node, err = hashicorpraft.NewRaft(config, metadataRaft.sc, metadataRaft.store, metadataRaft.store, snapshotStore, metadataRaft.transport)
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
		// Release first: the node's shutdown can emit a last leadership
		// notification, which still needs a consumer
		r.closeErr = r.release()
		close(r.shutdownCh)
	})
	return r.closeErr
}

const leadershipBarrierTimeout = 30 * time.Second

// waitToBecomeLeader blocks until this node becomes the raft leader, then
// waits for the FSM to catch up with all the committed entries (barrier), so
// that the first reads after a takeover are not stale. The returned channel
// is closed if the leadership is subsequently lost.
func (r *Raft) waitToBecomeLeader() (<-chan struct{}, error) {
	return waitForLeadership(r.notifyCh, r.shutdownCh, func() error {
		return r.node.Barrier(leadershipBarrierTimeout).Error()
	}, r.logger)
}

func waitForLeadership(notifyCh <-chan bool, shutdownCh <-chan struct{}, barrier func() error,
	logger *slog.Logger) (<-chan struct{}, error) {
	for {
		select {
		case isLeader := <-notifyCh:
			if !isLeader {
				// The channel notifies every transition, losses included:
				// keep waiting until this node actually wins
				continue
			}
			if err := barrier(); err != nil {
				// The leadership may already be gone: wait for the next term
				logger.Warn("Failed to wait for the FSM to catch up after becoming leader",
					slog.Any("error", err))
				continue
			}
			lost := make(chan struct{})
			go drainLeadershipNotifications(notifyCh, shutdownCh, lost)
			return lost, nil

		case <-shutdownCh:
			return nil, errors.New("raft provider closed while waiting for leadership")
		}
	}
}

// drainLeadershipNotifications closes lost on the first leadership loss and
// keeps consuming the notifications afterwards: raft blocks writing to the
// notification channel, so it must never be left without a consumer.
func drainLeadershipNotifications(notifyCh <-chan bool, shutdownCh <-chan struct{}, lost chan struct{}) {
	closed := false
	for {
		select {
		case isLeader := <-notifyCh:
			if !isLeader && !closed {
				close(lost)
				closed = true
			}
		case <-shutdownCh:
			return
		}
	}
}

// release shuts down whichever components have been created, in dependency
// order: the node first, then the store and the transport it was using.
func (r *Raft) release() error {
	var err error
	if r.node != nil {
		err = multierr.Append(err, r.node.Shutdown().Error())
	}
	if r.store != nil {
		err = multierr.Append(err, r.store.Close())
	}
	if r.transport != nil {
		err = multierr.Append(err, r.transport.Close())
	}
	return err
}

var _ io.Closer = (*Raft)(nil)
