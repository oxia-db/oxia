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
	"strconv"
	"sync"

	"github.com/oxia-db/oxia/coordinator/model"
)

var _ Provider = &metadataProviderMemory{}

// MetadataProviderMemory is a provider that just keeps the cluster status in memory
// Used for unit tests.
type metadataProviderMemory struct {
	sync.Mutex

	cs      *model.ClusterStatus
	version Version
}

func (*metadataProviderMemory) WaitToBecomeLeader() error {
	return nil
}

func NewMetadataProviderMemory() Provider {
	return &metadataProviderMemory{
		cs:      nil,
		version: NotExists,
	}
}

func (*metadataProviderMemory) Close() error {
	return nil
}

func (m *metadataProviderMemory) Get() (cs *model.ClusterStatus, version Version, err error) {
	m.Lock()
	defer m.Unlock()
	return m.cs, m.version, nil
}

func (m *metadataProviderMemory) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	m.Lock()
	defer m.Unlock()

	if expectedVersion != m.version {
		panic(ErrMetadataBadVersion)
	}

	m.cs = cs.Clone()
	m.version = incrVersion(m.version)
	return m.version, nil
}

func incrVersion(version Version) Version {
	i, err := strconv.ParseInt(string(version), 10, 64)
	if err != nil {
		return ""
	}
	i++
	return Version(strconv.FormatInt(i, 10))
}
