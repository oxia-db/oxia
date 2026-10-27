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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/multierr"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/file"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/raft"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

var _ raft.Interceptor = &Factory{}

type Factory struct {
	mu sync.Mutex

	closeOnce sync.Once
	closeErr  error

	statusProvider  provider.Provider[*commonproto.ClusterStatus]
	configProvider  provider.Provider[*commonproto.ClusterConfiguration]
	coordinatorName string
	// initialClusterConfig is set by New and never modified afterwards.
	initialClusterConfig *commonproto.ClusterConfiguration
	raft                 *raft.Raft
	raftInterceptor      raft.Interceptor
}

func (f *Factory) OnApplied(key string, data []byte, version int64) {
	if f.raftInterceptor == nil {
		return
	}
	f.raftInterceptor.OnApplied(key, data, version)
}

// New creates the metadata providers that options select. A non-nil
// initialClusterConfig is stored by the metadata that takes the leadership
// when the cluster has no configuration yet: it is validated here, so an
// invalid one is never stored.
func New(ctx context.Context, options *option.Options,
	initialClusterConfig *commonproto.ClusterConfiguration) (*Factory, error) {
	if options == nil {
		return nil, errors.New("options must not be nil")
	}
	if initialClusterConfig != nil {
		if err := initialClusterConfig.Validate(); err != nil {
			return nil, fmt.Errorf("invalid initial cluster configuration: %w", err)
		}
		initialClusterConfig = initialClusterConfig.CloneVT()
	}
	var err error
	meta := options.Metadata
	if err := meta.ApplyLegacyClusterConfigPath(options.Cluster.ConfigPath); err != nil {
		return nil, err
	}
	factory := &Factory{
		coordinatorName:      meta.Name,
		initialClusterConfig: initialClusterConfig,
	}
	switch meta.ProviderName {
	case metadatacommon.NameMemory:
		factory.statusProvider = memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, meta.Name) //nolint:contextcheck // the memory provider owns its context
		factory.configProvider = memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, meta.Name)  //nolint:contextcheck // the memory provider owns its context
	case metadatacommon.NameFile:
		if factory.statusProvider, err = file.NewProvider(ctx, meta.File.StatusPath(), metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, meta.Name); err != nil {
			return nil, err
		}
		if factory.configProvider, err = file.NewProvider(ctx, meta.File.ConfigPath(), metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, meta.Name); err != nil {
			return nil, err
		}
	case metadatacommon.NameConfigMap:
		client, err := kubernetes.NewDefaultClientset()
		if err != nil {
			return nil, err
		}
		factory.configProvider = kubernetes.NewProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.ConfigNameOrDefault(), metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, meta.Name)
		factory.statusProvider = kubernetes.NewProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.StatusNameOrDefault(), metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, meta.Name)
	case metadatacommon.NameRaft:
		if factory.raft, err = raft.New(meta.Raft.Address, meta.Raft.BootstrapNodes, meta.Raft.DataDir, factory); err != nil {
			return nil, fmt.Errorf("failed to create raft metadata provider: %w", err)
		}
		configProvider := raft.NewProvider(ctx, factory.raft, metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled)
		factory.configProvider = configProvider
		// Raft apply callbacks only drive the config watch today.
		// Status watch is intentionally unsupported; if we add it later,
		// this needs to fan out to the status provider too.
		factory.raftInterceptor = configProvider
		factory.statusProvider = raft.NewProvider(ctx, factory.raft, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	default:
		return nil, errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
	return factory, nil
}

func (f *Factory) CreateMetadata(ctx context.Context) Metadata {
	f.mu.Lock()
	statusProvider := f.statusProvider
	configProvider := f.configProvider
	coordinatorName := f.coordinatorName
	f.mu.Unlock()

	return newDefaultMetadata(ctx, statusProvider, configProvider, coordinatorName, f.initialClusterConfig)
}

// Close closes the metadata providers. It is safe to call more than once,
// and concurrently.
func (f *Factory) Close() error {
	f.closeOnce.Do(func() {
		f.closeErr = f.close()
	})
	return f.closeErr
}

func (f *Factory) close() error {
	f.mu.Lock()
	statusProvider := f.statusProvider
	configProvider := f.configProvider
	metadataRaft := f.raft
	f.mu.Unlock()

	var statusErr error
	if statusProvider != nil {
		statusErr = statusProvider.Close()
	}

	var configErr error
	if configProvider != nil {
		configErr = configProvider.Close()
	}

	var raftErr error
	if metadataRaft != nil {
		raftErr = metadataRaft.Close()
	}
	return multierr.Combine(statusErr, configErr, raftErr)
}
