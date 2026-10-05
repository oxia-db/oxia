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

package memory

import (
	"context"
	"sync"

	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider[*proto.ClusterStatus] = (*Provider[*proto.ClusterStatus])(nil)
var _ provider.Provider[*proto.ClusterConfiguration] = (*Provider[*proto.ClusterConfiguration])(nil)

type Provider[T gproto.Message] struct {
	mu           sync.Mutex
	codec        metadatacodec.Codec[T]
	value        T
	version      metadatacommon.Version
	watchEnabled metadatacommon.WatchMode
	name         string
	ctxCancel    context.CancelFunc
	cache        *cache.Cache[provider.Versioned[T]]
}

func (*Provider[T]) WaitToBecomeLeader() (<-chan struct{}, error) {
	// In-memory provider: single process, the leadership cannot be lost
	return nil, nil //nolint:nilnil
}

func (m *Provider[T]) GetLeaderName() (string, error) {
	return m.name, nil
}

func NewProvider[T gproto.Message](
	codec metadatacodec.Codec[T],
	watchEnabled metadatacommon.WatchMode,
	name string,
) provider.Provider[T] {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Provider[T]{
		codec:        codec,
		value:        codec.NewZero(),
		version:      metadatacommon.NotExists,
		watchEnabled: watchEnabled,
		name:         name,
		ctxCancel:    cancel,
	}
	p.cache = cache.New(ctx, p.load, nil)
	return p
}

func (m *Provider[T]) load(context.Context) (*provider.Versioned[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &provider.Versioned[T]{
		Value:   m.codec.Clone(m.value),
		Version: m.version,
	}, nil
}

func (m *Provider[T]) Close() error {
	m.ctxCancel()
	return m.cache.Close()
}

func (m *Provider[T]) write(snapshot provider.Versioned[T]) (*provider.Versioned[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if snapshot.Version != m.version {
		return nil, metadatacommon.ErrBadVersion
	}

	m.value = m.codec.Clone(snapshot.Value)
	m.version = metadatacommon.NextVersion(m.version)
	return &provider.Versioned[T]{
		Value:   m.codec.Clone(m.value),
		Version: m.version,
	}, nil
}

func (m *Provider[T]) Load() *provider.Versioned[T] {
	return m.cache.Get()
}

func (m *Provider[T]) Subscribe() *cache.Subscription[provider.Versioned[T]] {
	return m.cache.Subscribe()
}

// Store writes the snapshot, then updates the cache: a failed write empties
// it, since the write may still have been applied. The cache is updated after
// the write releases mu, which loading the cache takes. A load of the cache
// holds the cache lock until it stores its value, so a load that read the
// snapshot before the write cannot overwrite the cache update.
func (m *Provider[T]) Store(snapshot provider.Versioned[T]) (metadatacommon.Version, error) {
	stored, err := m.write(snapshot)
	if err != nil {
		m.cache.Invalidate()
		return metadatacommon.NotExists, err
	}
	m.cache.Set(stored)
	return stored.Version, nil
}
