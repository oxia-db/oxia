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
	"os"

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
	identity, err := newProviderIdentity(options)
	if err != nil {
		return nil, err
	}
	factory := &Factory{}
	switch meta.ProviderName {
	case metadatacommon.NameMemory:
		factory.statusProvider = memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, identity)
		factory.configProvider = memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, identity)
	case metadatacommon.NameFile:
		if factory.statusProvider, err = file.NewProvider(ctx, meta.File.StatusPath(), metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, identity); err != nil {
			return nil, err
		}
		if factory.configProvider, err = file.NewProvider(ctx, meta.File.ConfigPath(), metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, identity); err != nil {
			return nil, err
		}
	case metadatacommon.NameConfigMap:
		client, err := kubernetes.NewDefaultClientset()
		if err != nil {
			return nil, err
		}
		if factory.statusProvider, err = kubernetes.NewProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.StatusNameOrDefault(), metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, identity); err != nil {
			return nil, err
		}
		if factory.configProvider, err = kubernetes.NewProvider(ctx, client, meta.Kubernetes.Namespace, meta.Kubernetes.ConfigNameOrDefault(), metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, identity); err != nil {
			return nil, err
		}
	case metadatacommon.NameRaft:
		members := make(map[string]raft.Server, len(meta.Raft.Members)+1)
		for id, member := range meta.Raft.Members {
			members[id] = raft.Server{
				Address:       member.RaftAddress,
				PublicAddress: member.PublicAddress,
			}
		}
		if _, ok := members[identity.ID]; !ok {
			members[identity.ID] = raft.Server{
				Address:       meta.Raft.Address,
				PublicAddress: identity.PublicAddress,
			}
		}
		if factory.raft, err = raft.New(identity.ID, meta.Raft.Address, members, meta.Raft.DataDir, factory); err != nil {
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
	return newMetadata(ctx, f.statusProvider, f.configProvider), nil
}

func (f *Factory) WaitToBecomeLeader(ctx context.Context) error {
	slog.Info("Waiting to become leader", slog.String("component", "coordinator"))
	if err := f.statusProvider.WaitToBecomeLeader(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to wait in becoming leader: %w", err)
	}
	recoverStatus(ctx, slog.With(slog.String("component", "coordinator-metadata")), f.statusProvider)
	slog.Info("This coordinator is now leader", slog.String("component", "coordinator"))
	return nil
}

func (f *Factory) Close() error {
	statusProvider := f.statusProvider
	configProvider := f.configProvider
	metadataRaft := f.raft

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

func newProviderIdentity(options *option.Options) (provider.Identity, error) {
	id := options.Metadata.Identity
	if id == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return provider.Identity{}, err
		}
		id = hostname
	}
	return provider.Identity{
		ID:            id,
		PublicAddress: options.Server.Public.AdvertiseAddress,
	}, nil
}
