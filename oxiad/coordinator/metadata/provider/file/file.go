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

	"github.com/fsnotify/fsnotify"
	"github.com/juju/fslock"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type container struct {
	ClusterStatus json.RawMessage  `json:"clusterStatus,omitempty"`
	Version       provider.Version `json:"version,omitempty"`
}

var _ provider.Provider = &Provider{}

type Provider struct {
	path     string
	fileLock *fslock.Lock
	wrapped  bool
	version  provider.Version
}

func NewProvider(path string) provider.Provider {
	return &Provider{
		path:     path,
		fileLock: fslock.New(path),
		wrapped:  true,
		version:  provider.NotExists,
	}
}

func NewRawProvider(path string) provider.Provider {
	return &Provider{
		path:    path,
		version: provider.NotExists,
	}
}

func (m *Provider) Close() error {
	if m.fileLock == nil {
		return nil
	}
	if err := m.fileLock.Unlock(); err != nil {
		slog.Warn(
			"Failed to release file lock on metadata",
			slog.Any("error", err),
		)
	}

	return nil
}

func (m *Provider) WaitToBecomeLeader() error {
	if m.fileLock == nil {
		return nil
	}

	if err := m.ensureParentDirectoryExists(); err != nil {
		return err
	}

	if err := m.fileLock.Lock(); err != nil {
		return errors.Wrapf(err, "failed to acquire lock on %s", m.path)
	}

	return nil
}

func (m *Provider) Load() (data []byte, version provider.Version, err error) {
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
	if !m.wrapped {
		return content, m.version, nil
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

func (m *Provider) Store(data []byte, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	if err = m.ensureParentDirectoryExists(); err != nil {
		return provider.NotExists, err
	}

	_, existingVersion, err := m.Load()
	if err != nil {
		return provider.NotExists, err
	}

	if expectedVersion != existingVersion {
		panic(provider.ErrBadVersion)
	}

	newVersion = provider.NextVersion(existingVersion)
	newContent := data
	if m.wrapped {
		newContent, err = json.Marshal(container{
			ClusterStatus: data,
			Version:       newVersion,
		})
		if err != nil {
			return "", err
		}
	}

	if err := os.WriteFile(m.path, newContent, 0600); err != nil {
		return provider.NotExists, err
	}
	m.version = newVersion

	return newVersion, nil
}

func (m *Provider) Watch(ctx context.Context) (<-chan struct{}, error) {
	if err := m.ensureParentDirectoryExists(); err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := watcher.Add(filepath.Dir(m.path)); err != nil {
		_ = watcher.Close()
		return nil, err
	}

	notifyCh := make(chan struct{}, 1)
	go m.watch(ctx, watcher, notifyCh)

	return notifyCh, nil
}

func (m *Provider) watch(ctx context.Context, watcher *fsnotify.Watcher, notifyCh chan<- struct{}) {
	defer close(notifyCh)
	defer func() {
		if err := watcher.Close(); err != nil {
			slog.Warn("Failed to close file metadata watcher", slog.Any("error", err))
		}
	}()

	watchedPath := filepath.Clean(m.path)
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			slog.Warn("File metadata watch failed", slog.String("path", m.path), slog.Any("error", err))
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			notifyEvent(event, watchedPath, notifyCh)
		}
	}
}

func notifyEvent(event fsnotify.Event, watchedPath string, notifyCh chan<- struct{}) {
	if filepath.Clean(event.Name) != watchedPath {
		return
	}
	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename|fsnotify.Remove) == 0 {
		return
	}
	select {
	case notifyCh <- struct{}{}:
	default:
	}
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
