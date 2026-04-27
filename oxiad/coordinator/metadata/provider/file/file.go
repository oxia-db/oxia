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
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/fsnotify/fsnotify"
	"github.com/juju/fslock"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	metadatawatch "github.com/oxia-db/oxia/oxiad/coordinator/metadata/watch"
)

var _ provider.Provider = &Provider{}

type Provider struct {
	path         string
	resourceType provider.ResourceType
	fileLock     *fslock.Lock
	lockAcquired bool
	watchEnabled provider.WatchMode
	version      provider.Version

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	watcher *metadatawatch.Watch
}

func NewProvider(ctx context.Context, path string, resourceType provider.ResourceType, watchEnabled provider.WatchMode) (provider.Provider, error) {
	p := &Provider{
		path:         path,
		resourceType: resourceType,
		fileLock:     fslock.New(path),
		watchEnabled: watchEnabled,
		version:      provider.NotExists,
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	if err := p.ensureParentDirectoryExists(); err != nil {
		return nil, err
	}
	if watchEnabled.Enabled() {
		p.watcher = metadatawatch.New()
		p.wg.Go(func() {
			process.DoWithLabels(p.ctx, map[string]string{
				"component":     "metadata-provider",
				"sub-component": "file-watch",
			}, p.watchLoop)
		})
	}
	return p, nil
}

func (m *Provider) Close() error {
	m.cancel()
	m.wg.Wait()
	if m.watcher != nil {
		m.watcher.Close()
	}
	if !m.lockAcquired {
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
	if err := m.fileLock.Lock(); err != nil {
		return errors.Wrapf(err, "failed to acquire lock on %s", m.path)
	}
	m.lockAcquired = true

	return nil
}

func (m *Provider) Get() (value proto.Message, version provider.Version, err error) {
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
	value, err = m.resourceType.Unmarshal(content)
	return value, m.version, err
}

func (m *Provider) Store(value proto.Message, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	_, existingVersion, err := m.Get()
	if err != nil {
		return provider.NotExists, err
	}

	if expectedVersion != existingVersion {
		panic(provider.ErrBadVersion)
	}

	newVersion = provider.NextVersion(existingVersion)
	newContent, err := m.resourceType.MarshalYAML(value)
	if err != nil {
		return provider.NotExists, err
	}

	if err := os.WriteFile(m.path, newContent, 0600); err != nil {
		return provider.NotExists, err
	}
	m.version = newVersion

	return newVersion, nil
}

func (m *Provider) Watch() (*metadatawatch.Receiver, error) {
	if !m.watchEnabled.Enabled() || m.watcher == nil {
		return nil, provider.ErrWatchUnsupported
	}
	return m.watcher.Subscribe()
}

func (m *Provider) watchLoop() {
	retry := backoff.NewExponentialBackOff()
	retry.InitialInterval = time.Second
	_ = backoff.RetryNotify(func() error {
		value, _, err := m.Get()
		if err != nil {
			return err
		}
		m.watcher.Publish(value)
		return m.watchOnce()
	}, backoff.WithContext(retry, m.ctx), func(err error, duration time.Duration) {
		slog.Warn("File metadata watch failed, reconnecting",
			slog.String("path", m.path),
			slog.Any("error", err),
			slog.Duration("retry-after", duration))
	})
}

func (m *Provider) watchOnce() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			slog.Warn("Failed to close file metadata watcher", slog.Any("error", err))
		}
	}()

	if err := watcher.Add(filepath.Dir(m.path)); err != nil {
		return err
	}

	watchedPath := filepath.Clean(m.path)
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case err, ok := <-watcher.Errors:
			if !ok {
				return errors.New("file metadata watcher errors channel closed")
			}
			return err
		case event, ok := <-watcher.Events:
			if !ok {
				return errors.New("file metadata watcher events channel closed")
			}
			if filepath.Clean(event.Name) == watchedPath &&
				event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename|fsnotify.Remove) != 0 {
				value, _, err := m.Get()
				if err != nil {
					return err
				}
				m.watcher.Publish(value)
			}
		}
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
