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

package file

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/juju/fslock"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type container struct {
	ClusterStatus *model.ClusterStatus `json:"clusterStatus"`
	Version       provider.Version     `json:"version"`
}

var _ provider.Provider = &Provider{}

type Provider struct {
	path     string
	fileLock *fslock.Lock
}

func NewProvider(path string) provider.Provider {
	return &Provider{
		path:     path,
		fileLock: fslock.New(path),
	}
}

func (m *Provider) Close() error {
	if err := m.fileLock.Unlock(); err != nil {
		slog.Warn(
			"Failed to release file lock on metadata",
			slog.Any("error", err),
		)
	}

	return nil
}

func (m *Provider) WaitToBecomeLeader() error {
	if err := m.ensureParentDirectoryExists(); err != nil {
		return err
	}

	if err := m.fileLock.Lock(); err != nil {
		return errors.Wrapf(err, "failed to acquire lock on %s", m.path)
	}

	return nil
}

func (m *Provider) Get() (cs *model.ClusterStatus, version provider.Version, err error) {
	content, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, provider.NotExists, nil
		}
		return nil, provider.NotExists, err
	}

	if len(content) == 0 {
		return nil, provider.NotExists, nil
	}

	mc := container{}
	if err = json.Unmarshal(content, &mc); err != nil {
		return nil, provider.NotExists, err
	}

	return mc.ClusterStatus, mc.Version, nil
}

func (m *Provider) Store(cs *model.ClusterStatus, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	if err = m.ensureParentDirectoryExists(); err != nil {
		return provider.NotExists, err
	}

	_, existingVersion, err := m.Get()
	if err != nil {
		return provider.NotExists, err
	}

	if expectedVersion != existingVersion {
		panic(provider.ErrBadVersion)
	}

	newVersion = provider.NextVersion(existingVersion)
	newContent, err := json.Marshal(container{
		ClusterStatus: cs,
		Version:       newVersion,
	})
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(m.path, newContent, 0600); err != nil {
		return provider.NotExists, err
	}

	return newVersion, nil
}

func (m *Provider) ensureParentDirectoryExists() error {
	// Ensure directory exists
	parentDir := filepath.Dir(m.path)
	if _, err := os.Stat(parentDir); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return err
		}
	}

	return nil
}
