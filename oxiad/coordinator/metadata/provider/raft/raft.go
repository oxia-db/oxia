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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/magodo/slog2hclog"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider[*commonproto.ClusterStatus] = (*Provider[*commonproto.ClusterStatus])(nil)
var _ provider.Provider[*commonproto.ClusterConfiguration] = (*Provider[*commonproto.ClusterConfiguration])(nil)

type Provider[T gproto.Message] struct {
	resourceType provider.ResourceType
	codec        provider.Codec[T]
	raft         *Raft
}

type Raft struct {
	sync.Mutex
	sc    *stateContainer
	node  *hashicorpraft.Raft
	store *kvRaftStore
	log   *slog.Logger

	closeOnce sync.Once
	closeErr  error
}

func (mpr *Provider[T]) WaitToBecomeLeader() error {
	<-mpr.raft.node.LeaderCh()
	return nil
}

func newProvider[T gproto.Message](r *Raft, resourceType provider.ResourceType, codec provider.Codec[T]) provider.Provider[T] {
	return &Provider[T]{resourceType: resourceType, codec: codec, raft: r}
}

func NewProvider(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (provider.Provider[*commonproto.ClusterStatus], error) {
	metadataRaft, err := NewRaft(raftAddress, raftBootstrapNodes, raftDataDir)
	if err != nil {
		return nil, err
	}
	return newProvider(metadataRaft, provider.ResourceStatus, provider.ClusterStatusCodec), nil
}

func NewProviders(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (statusProvider provider.Provider[*commonproto.ClusterStatus], configProvider provider.Provider[*commonproto.ClusterConfiguration], err error) {
	metadataRaft, err := NewRaft(raftAddress, raftBootstrapNodes, raftDataDir)
	if err != nil {
		return nil, nil, err
	}
	return newProvider(metadataRaft, provider.ResourceStatus, provider.ClusterStatusCodec),
		newProvider(metadataRaft, provider.ResourceConfig, provider.ClusterConfigCodec),
		nil
}

func (r *Raft) NewStatusProvider() provider.Provider[*commonproto.ClusterStatus] {
	return newProvider(r, provider.ResourceStatus, provider.ClusterStatusCodec)
}

func (r *Raft) NewConfigProvider() provider.Provider[*commonproto.ClusterConfiguration] {
	return newProvider(r, provider.ResourceConfig, provider.ClusterConfigCodec)
}

func NewRaft(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (*Raft, error) {
	metadataRaft := &Raft{
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
	config := hashicorpraft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LocalID = hashicorpraft.ServerID(nodeId)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(metadataRaft.log, levelVar)

	// Create TCP transport for Raft
	addr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve raft address")
	}
	transport, err := hashicorpraft.NewTCPTransport(raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft transport")
	}

	// Create stable store and log store
	metadataRaft.store, err = newKVRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data store")
	}

	// Create snapshot store
	snapshotStore, err := hashicorpraft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store")
	}

	// Create Raft node
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

func (*Provider[T]) Close() error {
	return nil
}

func toVersion(v int64) provider.Version {
	return provider.Version(strconv.FormatInt(v, 10))
}

func fromVersion(v provider.Version) int64 {
	n, _ := strconv.ParseInt(string(v), 10, 64)
	return n
}

func (mpr *Provider[T]) Get() (value T, version provider.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	document := mpr.raft.sc.document(mpr.resourceType)

	mpr.raft.log.Debug("Get metadata",
		slog.String("resource-type", string(mpr.resourceType)),
		slog.Any("metadata", document.State),
		slog.Any("current-version", document.CurrentVersion))
	if len(document.State) == 0 {
		return value, toVersion(document.CurrentVersion), nil
	}
	value, err = mpr.codec.Unmarshal(document.State)
	return value, toVersion(document.CurrentVersion), err
}

func (mpr *Provider[T]) Store(value T, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	if err = mpr.raft.node.VerifyLeader().Error(); err != nil {
		return provider.NotExists, err
	}

	data, err := mpr.codec.MarshalJSON(value)
	if err != nil {
		return provider.NotExists, err
	}

	mpr.raft.log.Debug("Store into raft",
		slog.String("resource-type", string(mpr.resourceType)),
		slog.Any("metadata", data),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", mpr.raft.sc.document(mpr.resourceType).CurrentVersion))

	cmd := raftOpCmd{
		ResourceType:    mpr.resourceType,
		NewState:        json.RawMessage(data),
		ExpectedVersion: fromVersion(expectedVersion),
	}

	serializedCmd, err := json.Marshal(cmd)
	if err != nil {
		return provider.NotExists, err
	}

	future := mpr.raft.node.Apply(serializedCmd, 30*time.Second)
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

func (*Provider[T]) Watch() (*commonwatch.Receiver[T], error) {
	return nil, provider.ErrWatchUnsupported
}

var _ io.Closer = (*Raft)(nil)

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
