// Copyright 2023-2026 The Oxia Authors
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

	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	commonfile "github.com/oxia-db/oxia/oxiad/common/file"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider[*commonproto.ClusterStatus] = (*Provider[*commonproto.ClusterStatus])(nil)
var _ provider.Provider[*commonproto.ClusterConfiguration] = (*Provider[*commonproto.ClusterConfiguration])(nil)

const parentDirectoryMode = 0o755

type Provider[T gproto.Message] struct {
	mu           sync.Mutex
	path         string
	codec        metadatacodec.Codec[T]
	fileLock     *flock.Flock
	lockAcquired bool
	watchEnabled metadatacommon.WatchMode
	version      metadatacommon.Version
	name         string
	// existed is set once the file was read or written. A missing or empty
	// file is then an error rather than an empty snapshot: an empty
	// configuration would make the coordinator delete every namespace.
	existed bool

	ctx       context.Context
	ctxCancel context.CancelFunc

	cache  *cache.Cache[provider.Versioned[T]]
	logger *slog.Logger
}

func NewProvider[T gproto.Message](
	ctx context.Context,
	path string,
	codec metadatacodec.Codec[T],
	watchEnabled metadatacommon.WatchMode,
	name string,
) (provider.Provider[T], error) {
	p := &Provider[T]{
		path:         path,
		codec:        codec,
		fileLock:     flock.New(path),
		watchEnabled: watchEnabled,
		version:      metadatacommon.NotExists,
		name:         name,
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
	var watch cache.WatchFunc
	if watchEnabled.Enabled() {
		watch = func(ctx context.Context) (<-chan struct{}, error) {
			return commonfile.WatchFile(ctx, p.path)
		}
	}
	p.cache = cache.New(p.ctx, p.load, watch)
	return p, nil
}

func (m *Provider[T]) Close() error {
	m.ctxCancel()
	_ = m.cache.Close()
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

func (m *Provider[T]) WaitToBecomeLeader() (<-chan struct{}, error) {
	if err := m.fileLock.Lock(); err != nil {
		return nil, errors.Wrapf(err, "failed to acquire lock on %s", m.path)
	}
	m.lockAcquired = true

	// The file lock is held until the provider closes: no loss to signal
	return nil, nil //nolint:nilnil
}

func (m *Provider[T]) GetLeaderName() (string, error) {
	return m.name, nil
}

func (m *Provider[T]) load(context.Context) (*provider.Versioned[T], error) {
	snapshot, err := m.loadLatestOnceLocked()
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (m *Provider[T]) loadLatestOnceLocked() (snapshot provider.Versioned[T], err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.loadLatestOnceWithoutLock()
}

func (m *Provider[T]) loadLatestOnceWithoutLock() (snapshot provider.Versioned[T], err error) {
	content, err := os.ReadFile(m.path)
	if err != nil && !os.IsNotExist(err) {
		return snapshot, err
	}
	if len(content) == 0 {
		if m.existed {
			return snapshot, errors.Errorf("metadata file %s is missing or empty", m.path)
		}
		return provider.Versioned[T]{
			Value:   m.codec.NewZero(),
			Version: metadatacommon.NotExists,
		}, nil
	}
	m.existed = true
	value, err := m.codec.UnmarshalYAML(content)
	if err != nil {
		panic(err)
	}
	return provider.Versioned[T]{
		Value:   value,
		Version: m.version,
	}, nil
}

func (m *Provider[T]) write(snapshot provider.Versioned[T]) (*provider.Versioned[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existingSnapshot, err := m.loadLatestOnceWithoutLock()
	if err != nil {
		return nil, err
	}
	existingVersion := existingSnapshot.Version

	if snapshot.Version != existingVersion {
		return nil, metadatacommon.ErrBadVersion
	}

	newVersion := metadatacommon.NextVersion(existingVersion)
	newContent, err := m.codec.MarshalYAML(snapshot.Value)
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(m.path, newContent, 0600); err != nil {
		return nil, err
	}
	m.version = newVersion
	m.existed = true
	return &provider.Versioned[T]{
		Value:   m.codec.Clone(snapshot.Value),
		Version: newVersion,
	}, nil
}

func (m *Provider[T]) Load() (*provider.Versioned[T], error) {
	return m.cache.Get()
}

func (m *Provider[T]) Subscribe() *cache.Subscription[provider.Versioned[T]] {
	return m.cache.Subscribe()
}

// Store writes the snapshot through the cache, so that no load of the cache
// stores a snapshot read before the write. When the write fails, which it may
// do after being applied, the next Load or Store reads the stored snapshot.
func (m *Provider[T]) Store(snapshot provider.Versioned[T]) (metadatacommon.Version, error) {
	stored, err := m.cache.Compute(func(*provider.Versioned[T]) (*provider.Versioned[T], error) {
		return m.write(snapshot)
	})
	if err != nil {
		return metadatacommon.NotExists, err
	}
	return stored.Version, nil
}

func (m *Provider[T]) Reload() error {
	return m.cache.Reload()
}
