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

package memory

import (
	"sync"

	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider[*proto.ClusterStatus] = (*Provider[*proto.ClusterStatus])(nil)
var _ provider.Provider[*proto.ClusterConfiguration] = (*Provider[*proto.ClusterConfiguration])(nil)

type Provider[T gproto.Message] struct {
	mu           sync.Mutex
	codec        metadatacommon.Codec[T]
	value        T
	version      metadatacommon.Version
	watchEnabled metadatacommon.WatchMode
	watch        *commonwatch.Watch[provider.Versioned[T]]
}

func (*Provider[T]) WaitToBecomeLeader() error {
	return nil
}

func NewProvider[T gproto.Message](codec metadatacommon.Codec[T], watchEnabled metadatacommon.WatchMode) provider.Provider[T] {
	p := &Provider[T]{
		codec:        codec,
		value:        codec.NewZero(),
		version:      metadatacommon.NotExists,
		watchEnabled: watchEnabled,
		watch: commonwatch.New(provider.Versioned[T]{
			Value:   codec.NewZero(),
			Version: metadatacommon.NotExists,
		}),
	}
	return p
}

func (*Provider[T]) Close() error {
	return nil
}

func (m *Provider[T]) Store(snapshot provider.Versioned[T]) (newVersion metadatacommon.Version, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if snapshot.Version != m.version {
		return metadatacommon.NotExists, metadatacommon.ErrBadVersion
	}

	m.value = m.codec.Clone(snapshot.Value)
	m.version = metadatacommon.NextVersion(m.version)
	m.watch.Publish(provider.Versioned[T]{
		Value:   m.codec.Clone(m.value),
		Version: m.version,
	})
	return m.version, nil
}

func (m *Provider[T]) Watch() *commonwatch.Watch[provider.Versioned[T]] {
	return m.watch
}
