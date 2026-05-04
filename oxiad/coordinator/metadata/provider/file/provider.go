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
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider[*commonproto.ClusterStatus] = (*Provider[*commonproto.ClusterStatus])(nil)
var _ provider.Provider[*commonproto.ClusterConfiguration] = (*Provider[*commonproto.ClusterConfiguration])(nil)

const parentDirectoryMode = 0o755

type Provider[T gproto.Message] struct {
	path         string
	codec        metadatacommon.Codec[T]
	fileLock     *fslock.Lock
	lockAcquired bool
	watchEnabled metadatacommon.WatchMode
	version      metadatacommon.Version

	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup

	watcher *commonwatch.Watch[provider.Versioned[T]]
	logger  *slog.Logger
}

func NewProvider[T gproto.Message](ctx context.Context, path string, codec metadatacommon.Codec[T], watchEnabled metadatacommon.WatchMode) (provider.Provider[T], error) {
	p := &Provider[T]{
		path:         path,
		codec:        codec,
		fileLock:     fslock.New(path),
		watchEnabled: watchEnabled,
		version:      metadatacommon.NotExists,
		logger:       slog.With(slog.String("component", "metadata-file-provider"), slog.String("path", path)),
	}
	p.ctx, p.ctxCancel = context.WithCancel(ctx)
	parentDir := filepath.Dir(path)
	if _, err := os.Stat(parentDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.MkdirAll(parentDir, parentDirectoryMode); err != nil {
			return nil, err
		}
	}
	initialSnapshot, err := p.loadLatest()
	if err != nil {
		return nil, err
	}
	p.watcher = commonwatch.New(initialSnapshot)
	if watchEnabled.Enabled() {
		p.wg.Go(func() {
			process.DoWithLabels(p.ctx, map[string]string{
				"component":     "metadata-provider",
				"sub-component": "file-watch",
			}, p.watchLoop)
		})
	}
	return p, nil
}

func (m *Provider[T]) Close() error {
	m.ctxCancel()
	m.wg.Wait()
	if !m.lockAcquired {
		return nil
	}
	if err := m.fileLock.Unlock(); err != nil {
		m.logger.Warn(
			"Failed to release file lock on metadata",
			slog.Any("error", err),
		)
	}

	return nil
}

func (m *Provider[T]) WaitToBecomeLeader() error {
	if err := m.fileLock.Lock(); err != nil {
		return errors.Wrapf(err, "failed to acquire lock on %s", m.path)
	}
	m.lockAcquired = true

	return nil
}

func (m *Provider[T]) loadLatest() (snapshot provider.Versioned[T], err error) {
	err = backoff.RetryNotify(func() error {
		var readErr error
		snapshot, readErr = m.loadLatestOnce()
		return readErr
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn("Failed to read file metadata, retrying",
			slog.Any("error", err),
			slog.Duration("retry-after", duration))
	})
	return snapshot, err
}

func (m *Provider[T]) loadLatestOnce() (snapshot provider.Versioned[T], err error) {
	content, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return provider.Versioned[T]{
				Value:   m.codec.NewZero(),
				Version: metadatacommon.NotExists,
			}, nil
		}
		return snapshot, err
	}

	if len(content) == 0 {
		return provider.Versioned[T]{
			Value:   m.codec.NewZero(),
			Version: metadatacommon.NotExists,
		}, nil
	}
	value, err := m.codec.UnmarshalYAML(content)
	if err != nil {
		panic(err)
	}
	return provider.Versioned[T]{
		Value:   value,
		Version: m.version,
	}, nil
}

func (m *Provider[T]) Store(snapshot provider.Versioned[T]) (newVersion metadatacommon.Version, err error) {
	existingSnapshot, err := m.loadLatest()
	if err != nil {
		return metadatacommon.NotExists, err
	}
	existingVersion := existingSnapshot.Version

	if snapshot.Version != existingVersion {
		return metadatacommon.NotExists, metadatacommon.ErrBadVersion
	}

	newVersion = metadatacommon.NextVersion(existingVersion)
	newContent, err := m.codec.MarshalYAML(snapshot.Value)
	if err != nil {
		return metadatacommon.NotExists, err
	}

	if err := os.WriteFile(m.path, newContent, 0600); err != nil {
		return metadatacommon.NotExists, err
	}
	m.version = newVersion
	m.watcher.Publish(provider.Versioned[T]{
		Value:   m.codec.Clone(snapshot.Value),
		Version: newVersion,
	})

	return newVersion, nil
}

func (m *Provider[T]) Watch() *commonwatch.Watch[provider.Versioned[T]] {
	return m.watcher
}

func (m *Provider[T]) watchLoop() {
	_ = backoff.RetryNotify(func() error {
		snapshot, err := m.loadLatest()
		if err != nil {
			return err
		}
		m.watcher.Publish(snapshot)
		return m.watchOnce()
	}, oxiatime.NewBackOffWithInitialInterval(m.ctx, time.Second), func(err error, duration time.Duration) {
		m.logger.Warn("File metadata watch failed, reconnecting",
			slog.String("path", m.path),
			slog.Any("error", err),
			slog.Duration("retry-after", duration))
	})
}

func (m *Provider[T]) watchOnce() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			m.logger.Warn("Failed to close file metadata watcher", slog.Any("error", err))
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
				snapshot, err := m.loadLatest()
				if err != nil {
					return err
				}
				m.watcher.Publish(snapshot)
			}
		}
	}
}
