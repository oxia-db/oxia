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

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider = &Provider{}

type Provider struct {
	sync.Mutex

	documents map[provider.Document]*document
	watch     chan struct{}
}

type document struct {
	data    []byte
	version provider.Version
}

func (*Provider) WaitToBecomeLeader() error {
	return nil
}

func NewProvider() provider.Provider {
	return &Provider{
		documents: map[provider.Document]*document{},
	}
}

func (*Provider) Close() error {
	return nil
}

func (m *Provider) Load(name provider.Document) (data []byte, version provider.Version, err error) {
	m.Lock()
	defer m.Unlock()

	doc, ok := m.documents[name]
	if !ok {
		return nil, provider.NotExists, nil
	}

	return append([]byte(nil), doc.data...), doc.version, nil
}

func (m *Provider) Store(name provider.Document, data []byte, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	m.Lock()
	defer m.Unlock()

	version := provider.NotExists
	doc, ok := m.documents[name]
	if ok {
		version = doc.version
	}

	if expectedVersion != version {
		panic(provider.ErrBadVersion)
	}

	newVersion = provider.NextVersion(version)
	m.documents[name] = &document{
		data:    append([]byte(nil), data...),
		version: newVersion,
	}
	if name == provider.DocumentClusterConfiguration && m.watch != nil {
		select {
		case m.watch <- struct{}{}:
		default:
		}
	}
	return newVersion, nil
}

func (*Provider) SupportsWatch() bool {
	return true
}

func (m *Provider) Watch() <-chan struct{} {
	m.Lock()
	defer m.Unlock()

	if m.watch == nil {
		m.watch = make(chan struct{}, 1)
	}
	return m.watch
}
