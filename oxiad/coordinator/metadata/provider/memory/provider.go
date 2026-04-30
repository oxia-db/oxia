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
	sync.Mutex

	codec        metadatacommon.Codec[T]
	value        T
	version      metadatacommon.Version
	watchEnabled metadatacommon.WatchMode
	watch        *commonwatch.Watch[T]
}

func (*Provider[T]) WaitToBecomeLeader() error {
	return nil
}

func NewProvider[T gproto.Message](codec metadatacommon.Codec[T], watchEnabled metadatacommon.WatchMode) provider.Provider[T] {
	p := &Provider[T]{
		codec:        codec,
		version:      metadatacommon.NotExists,
		watchEnabled: watchEnabled,
	}
	if watchEnabled.Enabled() {
		p.watch = commonwatch.New(codec.NewZero())
	}
	return p
}

func (*Provider[T]) Close() error {
	return nil
}

func (m *Provider[T]) Get() (value T, version metadatacommon.Version, err error) {
	m.Lock()
	defer m.Unlock()

	return m.codec.Clone(m.value), m.version, nil
}

func (m *Provider[T]) Store(value T, expectedVersion metadatacommon.Version) (newVersion metadatacommon.Version, err error) {
	m.Lock()
	defer m.Unlock()

	if expectedVersion != m.version {
		panic(metadatacommon.ErrBadVersion)
	}

	m.value = m.codec.Clone(value)
	m.version = metadatacommon.NextVersion(m.version)
	if m.watch != nil {
		m.watch.Publish(m.codec.Clone(m.value))
	}
	return m.version, nil
}

func (m *Provider[T]) Watch() (*commonwatch.Receiver[T], error) {
	if !m.watchEnabled.Enabled() || m.watch == nil {
		return nil, metadatacommon.ErrWatchUnsupported
	}
	return m.watch.Subscribe(), nil
}
