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
	"google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	metadatawatch "github.com/oxia-db/oxia/oxiad/coordinator/metadata/watch"
)

type Provider struct {
	resourceType provider.ResourceType
	backend      *backend
}

type backend struct {
	sync.Mutex
	sc    *stateContainer
	raft  *raft.Raft
	store *kvRaftStore
	log   *slog.Logger

	closeOnce sync.Once
	closeErr  error
}

func (mpr *Provider) WaitToBecomeLeader() error {
	<-mpr.backend.raft.LeaderCh()
	return nil
}

func NewProvider(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (provider.Provider, error) {
	statusProvider, _, err := NewProviders(raftAddress, raftBootstrapNodes, raftDataDir)
	return statusProvider, err
}

func NewProviders(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (statusProvider provider.Provider, configProvider provider.Provider, err error) {
	backend := &backend{
		sc:  newStateContainer(slog.With(slog.String("component", "metadata-provider-raft-state-container"))),
		log: slog.With(slog.String("component", "metadata-provider-raft")),
	}

	// Ensure data dir per node
	nodeId := raftAddress
	dataDir := filepath.Join(raftDataDir, nodeId)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, nil, errors.Wrap(err, "failed to create data dir")
	}

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LocalID = raft.ServerID(nodeId)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(backend.log, levelVar)

	// Create TCP transport for Raft
	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to resolve raft address")
	}
	transport, err := raft.NewTCPTransport(raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create raft transport")
	}

	// Create stable store and log store
	backend.store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create data store")
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create snapshot store")
	}

	// Create Raft node
	backend.raft, err = raft.NewRaft(config, backend.sc, backend.store, backend.store, snapshotStore, transport)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create raft node")
	}

	if hasState, err := raft.HasExistingState(backend.store, backend.store, snapshotStore); err != nil {
		return nil, nil, errors.Wrap(err, "failed to check existing state")
	} else if !hasState {
		configuration := raft.Configuration{
			Servers: getRaftServers(raftBootstrapNodes),
		}
		future := backend.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return nil, nil, errors.Wrap(err, "failed to create raft node")
		}
	}

	return &Provider{resourceType: provider.ResourceStatus, backend: backend},
		&Provider{resourceType: provider.ResourceConfig, backend: backend},
		nil
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

func (mpr *Provider) Close() error {
	mpr.backend.closeOnce.Do(func() {
		mpr.backend.closeErr = multierr.Combine(
			mpr.backend.raft.Shutdown().Error(),
			mpr.backend.store.Close(),
		)
	})
	return mpr.backend.closeErr
}

func toVersion(v int64) provider.Version {
	return provider.Version(strconv.FormatInt(v, 10))
}

func fromVersion(v provider.Version) int64 {
	n, _ := strconv.ParseInt(string(v), 10, 64)
	return n
}

func (mpr *Provider) Get() (value proto.Message, version provider.Version, err error) {
	mpr.backend.Lock()
	defer mpr.backend.Unlock()

	document := mpr.backend.sc.document(mpr.resourceType)

	mpr.backend.log.Debug("Get metadata",
		slog.String("resource-type", string(mpr.resourceType)),
		slog.Any("metadata", document.State),
		slog.Any("current-version", document.CurrentVersion))
	if len(document.State) == 0 {
		return nil, toVersion(document.CurrentVersion), nil
	}
	value, err = mpr.resourceType.Unmarshal(document.State)
	return value, toVersion(document.CurrentVersion), err
}

func (mpr *Provider) Store(value proto.Message, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	mpr.backend.Lock()
	defer mpr.backend.Unlock()

	if err = mpr.backend.raft.VerifyLeader().Error(); err != nil {
		return provider.NotExists, err
	}

	data, err := mpr.resourceType.MarshalJSON(value)
	if err != nil {
		return provider.NotExists, err
	}

	mpr.backend.log.Debug("Store into raft",
		slog.String("resource-type", string(mpr.resourceType)),
		slog.Any("metadata", data),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", mpr.backend.sc.document(mpr.resourceType).CurrentVersion))

	cmd := raftOpCmd{
		ResourceType:    mpr.resourceType,
		NewState:        json.RawMessage(data),
		ExpectedVersion: fromVersion(expectedVersion),
	}

	serializedCmd, err := json.Marshal(cmd)
	if err != nil {
		return provider.NotExists, err
	}

	future := mpr.backend.raft.Apply(serializedCmd, 30*time.Second)
	if err := future.Error(); err != nil {
		return provider.NotExists, errors.Wrap(err, "failed to apply new cluster state")
	}

	applyRes, ok := future.Response().(*applyResult)
	if !ok {
		return provider.NotExists, errors.Wrap(err, "failed to apply new cluster state")
	}

	if !applyRes.changeApplied {
		panic(provider.ErrBadVersion)
	}

	return toVersion(applyRes.newVersion), nil
}

func (*Provider) Watch() (*metadatawatch.Receiver, error) {
	return nil, provider.ErrWatchUnsupported
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
