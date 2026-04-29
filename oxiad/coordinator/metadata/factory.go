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
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/file"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/raft"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

type Factory struct {
	mu sync.Mutex

	statusProvider provider.Provider[*commonproto.ClusterStatus]
	configProvider provider.Provider[*commonproto.ClusterConfiguration]
	metadataRaft   *raft.Raft
}

func NewFactoryWithCallbackConfig(
	ctx context.Context,
	statusProvider provider.Provider[*commonproto.ClusterStatus],
	clusterConfigProvider func() (*commonproto.ClusterConfiguration, error),
	clusterConfigNotificationsCh <-chan any,
) *Factory {
	return &Factory{
		statusProvider: statusProvider,
		configProvider: newCallbackConfigProvider(ctx, clusterConfigProvider, clusterConfigNotificationsCh),
	}
}

func New(ctx context.Context, options *option.Options) (*Factory, error) {
	if options == nil {
		return nil, errors.New("options must not be nil")
	}

	meta := options.Metadata
	if err := meta.ApplyLegacyClusterConfigPath(options.Cluster.ConfigPath); err != nil {
		return nil, err
	}

	factory := &Factory{}

	switch meta.ProviderName {
	case provider.NameMemory:
		factory.statusProvider = memory.NewProvider(provider.ClusterStatusCodec)
		factory.configProvider = memory.NewProvider(provider.ClusterConfigCodec)
	case provider.NameFile:
		statusProvider, err := file.NewProvider(ctx, meta.File.StatusPath(), provider.ClusterStatusCodec, provider.WatchDisabled)
		if err != nil {
			return nil, err
		}
		configProvider, err := file.NewProvider(ctx, meta.File.ConfigPath(), provider.ClusterConfigCodec, provider.WatchEnabled)
		if err != nil {
			return nil, err
		}
		factory.statusProvider = statusProvider
		factory.configProvider = configProvider
	case provider.NameConfigMap:
		k8sConfig := kubernetes.NewK8SClientConfig()
		client := kubernetes.NewK8SClientset(k8sConfig)
		statusProvider, err := kubernetes.NewConfigMapProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.StatusNameOrDefault(), provider.ClusterStatusCodec, provider.WatchDisabled)
		if err != nil {
			return nil, err
		}
		factory.statusProvider = statusProvider
		if meta.File.Dir != "" || meta.File.ConfigName != "" {
			configProvider, err := file.NewProvider(ctx, meta.File.ConfigPath(), provider.ClusterConfigCodec, provider.WatchEnabled)
			if err != nil {
				return nil, err
			}
			factory.configProvider = configProvider
		} else {
			configProvider, err := kubernetes.NewConfigMapProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.ConfigNameOrDefault(), provider.ClusterConfigCodec, provider.WatchEnabled)
			if err != nil {
				return nil, err
			}
			factory.configProvider = configProvider
		}
	case provider.NameRaft:
		metadataRaft, err := raft.NewRaft(meta.Raft.Address, meta.Raft.BootstrapNodes, meta.Raft.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create raft metadata provider: %w", err)
		}
		factory.metadataRaft = metadataRaft
		factory.statusProvider = metadataRaft.NewStatusProvider()
		factory.configProvider = metadataRaft.NewConfigProvider()
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
	metadataRaft := f.metadataRaft
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
