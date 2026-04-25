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
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/juju/fslock"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type container struct {
	ClusterStatus json.RawMessage  `json:"clusterStatus"`
	Version       provider.Version `json:"version"`
}

var _ provider.Provider = &Provider{}

type Provider struct {
	statusPath string
	configPath string
	fileLock   *fslock.Lock

	ctx          context.Context
	cancel       context.CancelFunc
	configWatch  chan struct{}
	configWatchM sync.Mutex
}

func NewProvider(path string) provider.Provider {
	return NewProviderWithPaths(path, path)
}

func NewProviderWithPaths(statusPath, configPath string) provider.Provider {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Provider{
		statusPath: statusPath,
		configPath: configPath,
		ctx:        ctx,
		cancel:     cancel,
	}
	if statusPath != "" {
		p.fileLock = fslock.New(statusPath)
	}
	return p
}

func (m *Provider) Close() error {
	m.cancel()
	if m.fileLock != nil {
		if err := m.fileLock.Unlock(); err != nil {
			slog.Warn(
				"Failed to release file lock on metadata",
				slog.Any("error", err),
			)
		}
	}

	return nil
}

func (m *Provider) WaitToBecomeLeader() error {
	if m.statusPath == "" {
		return nil
	}
	if err := ensureParentDirectoryExists(m.statusPath); err != nil {
		return err
	}

	if err := m.fileLock.Lock(); err != nil {
		return errors.Wrapf(err, "failed to acquire lock on %s", m.statusPath)
	}

	return nil
}

func (m *Provider) path(document provider.Document) (string, error) {
	switch document {
	case provider.DocumentClusterStatus:
		if m.statusPath == "" {
			return "", errors.New("cluster status file path is not configured")
		}
		return m.statusPath, nil
	case provider.DocumentClusterConfiguration:
		if m.configPath == "" {
			return "", errors.New("cluster configuration file path is not configured")
		}
		return m.configPath, nil
	default:
		return "", errors.Errorf("unsupported metadata document %q", document)
	}
}

func (m *Provider) Load(document provider.Document) (data []byte, version provider.Version, err error) {
	switch document {
	case provider.DocumentClusterStatus:
		return m.loadStatus()
	case provider.DocumentClusterConfiguration:
		path, err := m.path(document)
		if err != nil {
			return nil, provider.NotExists, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, provider.NotExists, nil
			}
			return nil, provider.NotExists, err
		}
		return data, provider.NotExists, nil
	default:
		return nil, provider.NotExists, errors.Errorf("unsupported metadata document %q", document)
	}
}

func (m *Provider) loadStatus() (data []byte, version provider.Version, err error) {
	path, err := m.path(provider.DocumentClusterStatus)
	if err != nil {
		return nil, provider.NotExists, err
	}
	content, err := os.ReadFile(path)
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

	if len(mc.ClusterStatus) == 0 {
		return nil, mc.Version, nil
	}

	return mc.ClusterStatus, mc.Version, nil
}

func (m *Provider) Store(document provider.Document, data []byte, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	switch document {
	case provider.DocumentClusterStatus:
		return m.storeStatus(data, expectedVersion)
	case provider.DocumentClusterConfiguration:
		path, err := m.path(document)
		if err != nil {
			return provider.NotExists, err
		}
		if err = ensureParentDirectoryExists(path); err != nil {
			return provider.NotExists, err
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			return provider.NotExists, err
		}
		m.notifyConfigWatch()
		return provider.NotExists, nil
	default:
		return provider.NotExists, errors.Errorf("unsupported metadata document %q", document)
	}
}

func (m *Provider) storeStatus(data []byte, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	path, err := m.path(provider.DocumentClusterStatus)
	if err != nil {
		return provider.NotExists, err
	}
	if err = ensureParentDirectoryExists(path); err != nil {
		return provider.NotExists, err
	}

	_, existingVersion, err := m.loadStatus()
	if err != nil {
		return provider.NotExists, err
	}

	if expectedVersion != existingVersion {
		panic(provider.ErrBadVersion)
	}

	newVersion = provider.NextVersion(existingVersion)
	newContent, err := json.Marshal(container{
		ClusterStatus: data,
		Version:       newVersion,
	})
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(path, newContent, 0600); err != nil {
		return provider.NotExists, err
	}

	return newVersion, nil
}

func (*Provider) SupportsWatch() bool {
	return true
}

func (m *Provider) Watch() <-chan struct{} {
	m.configWatchM.Lock()
	defer m.configWatchM.Unlock()
	if m.configWatch != nil {
		return m.configWatch
	}

	m.configWatch = make(chan struct{}, 1)
	go m.watchConfigFile()
	return m.configWatch
}

func (m *Provider) watchConfigFile() {
	path, err := m.path(provider.DocumentClusterConfiguration)
	if err != nil {
		slog.Warn(
			"failed to watch file metadata",
			slog.Any("error", err),
		)
		return
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		slog.Warn("failed to create file metadata watcher", slog.Any("error", err))
		return
	}
	defer watcher.Close()

	if err := watcher.Add(path); err != nil {
		slog.Warn("failed to watch file metadata", slog.String("path", path), slog.Any("error", err))
		return
	}

	for {
		select {
		case <-m.ctx.Done():
			return
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			slog.Warn("file metadata watch failed", slog.String("path", path), slog.Any("error", err))
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				m.notifyConfigWatch()
			}
		}
	}
}

func (m *Provider) notifyConfigWatch() {
	m.configWatchM.Lock()
	ch := m.configWatch
	m.configWatchM.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func ensureParentDirectoryExists(path string) error {
	parentDir := filepath.Dir(path)
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
