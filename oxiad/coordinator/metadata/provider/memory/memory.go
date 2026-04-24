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

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider = &Provider{}

type Provider struct {
	sync.Mutex

	cs      *commonproto.ClusterStatus
	version provider.Version
}

func (*Provider) WaitToBecomeLeader() error {
	return nil
}

func NewProvider() provider.Provider {
	return &Provider{
		cs:      nil,
		version: provider.NotExists,
	}
}

func (*Provider) Close() error {
	return nil
}

func (m *Provider) Get() (cs *commonproto.ClusterStatus, version provider.Version, err error) {
	m.Lock()
	defer m.Unlock()
	return m.cs, m.version, nil
}

func (m *Provider) Store(cs *commonproto.ClusterStatus, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	m.Lock()
	defer m.Unlock()

	if expectedVersion != m.version {
		panic(provider.ErrBadVersion)
	}

	m.cs = gproto.Clone(cs).(*commonproto.ClusterStatus) //nolint:revive
	m.version = provider.NextVersion(m.version)
	return m.version, nil
}
