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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"go.uber.org/multierr"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
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

	statusProvider  provider.Provider[*commonproto.ClusterStatus]
	configProvider  provider.Provider[*commonproto.ClusterConfiguration]
	raft            *raft.Raft
	raftInterceptor raft.Interceptor
}

func (f *Factory) OnApplied(key string, data []byte, version int64) {
	if f.raftInterceptor == nil {
		return
	}
	f.raftInterceptor.OnApplied(key, data, version)
}

func New(ctx context.Context, options *option.Options) (*Factory, error) {
	if options == nil {
		return nil, errors.New("options must not be nil")
	}
	var err error
	meta := options.Metadata
	if err := meta.ApplyLegacyClusterConfigPath(options.Cluster.ConfigPath); err != nil {
		return nil, err
	}
	factory := &Factory{}
	switch meta.ProviderName {
	case metadatacommon.NameMemory:
		factory.statusProvider = memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
		factory.configProvider = memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled)
	case metadatacommon.NameFile:
		if factory.statusProvider, err = file.NewProvider(ctx, meta.File.StatusPath(), metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled); err != nil {
			return nil, err
		}
		if factory.configProvider, err = file.NewProvider(ctx, meta.File.ConfigPath(), metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled); err != nil {
			return nil, err
		}
	case metadatacommon.NameConfigMap:
		client, err := kubernetes.NewDefaultClientset()
		if err != nil {
			return nil, err
		}
		if factory.statusProvider, err = kubernetes.NewProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.StatusNameOrDefault(), metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled); err != nil {
			return nil, err
		}
		if factory.configProvider, err = kubernetes.NewProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.ConfigNameOrDefault(), metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled); err != nil {
			return nil, err
		}
	case metadatacommon.NameRaft:
		if factory.raft, err = raft.New(meta.Raft.Address, meta.Raft.BootstrapNodes, meta.Raft.DataDir, factory); err != nil {
			return nil, fmt.Errorf("failed to create raft metadata provider: %w", err)
		}
		factory.configProvider = raft.NewProvider(ctx, factory.raft, metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled)
		// Raft apply callbacks only drive the config watch today.
		// Status watch is intentionally unsupported; if we add it later,
		// this needs to fan out to the status provider too.
		configProvider, ok := factory.configProvider.(*raft.Provider[*commonproto.ClusterConfiguration])
		if !ok {
			return nil, errors.New("failed to create raft config provider")
		}
		factory.raftInterceptor = configProvider
		factory.statusProvider = raft.NewProvider(ctx, factory.raft, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	default:
		return nil, errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
	return factory, nil
}

func (f *Factory) CreateMetadata(ctx context.Context) (Metadata, error) {
	f.mu.Lock()
	statusProvider := f.statusProvider
	configProvider := f.configProvider
	f.mu.Unlock()

	slog.Info("Waiting to become leader", slog.String("component", "coordinator"))
	if err := statusProvider.WaitToBecomeLeader(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to wait in becoming leader: %w", err)
	}
	slog.Info("This coordinator is now leader", slog.String("component", "coordinator"))

	return newMetadata(ctx, statusProvider, configProvider), nil
}

func (f *Factory) Close() error {
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
