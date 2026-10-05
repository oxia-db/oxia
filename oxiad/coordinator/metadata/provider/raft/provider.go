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

package raft

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Provider[T gproto.Message] struct {
	codec     metadatacodec.Codec[T]
	raft      *Raft
	watchMode metadatacommon.WatchMode
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	// changes is signaled by OnApplied, and coalesces the signals.
	changes chan struct{}
	cache   *cache.Cache[provider.Versioned[T]]
	logger  *slog.Logger
}

func NewProvider[T gproto.Message](ctx context.Context, r *Raft, codec metadatacodec.Codec[T], watchEnabled metadatacommon.WatchMode) provider.Provider[T] {
	ctx, cancel := context.WithCancel(ctx)
	p := &Provider[T]{
		codec:     codec,
		raft:      r,
		watchMode: watchEnabled,
		ctx:       ctx,
		ctxCancel: cancel,
		changes:   make(chan struct{}, 1),
		logger: slog.With(
			slog.String("component", "metadata-provider-raft-watch"),
		),
	}

	p.cache = cache.New(ctx, p.load, func(context.Context) (<-chan struct{}, error) {
		return p.changes, nil
	})
	return p
}

// OnApplied reloads the cache from the applied state, which also covers the
// entries replicated from another leader. The cache is emptied before
// OnApplied returns, so a read once the entry is applied, like after the
// leadership barrier, loads the applied state instead of the cached one.
func (mpr *Provider[T]) OnApplied(key string, _ []byte, _ int64) {
	if mpr.codec.GetKey() != key {
		return
	}
	mpr.cache.Invalidate()
	channel.PushNoBlock(mpr.changes, struct{}{})
}

func (mpr *Provider[T]) WaitToBecomeLeader() (<-chan struct{}, error) {
	return mpr.raft.waitToBecomeLeader()
}

func (*Provider[T]) GetLeaderName() (string, error) {
	return "", provider.ErrCoordinatorLeaderUnavailable
}

func (mpr *Provider[T]) Close() error {
	mpr.ctxCancel()
	_ = mpr.cache.Close()
	mpr.wg.Wait()
	return nil
}

func toVersion(v int64) metadatacommon.Version {
	return metadatacommon.Version(strconv.FormatInt(v, 10))
}

func fromVersion(v metadatacommon.Version) int64 {
	n, _ := strconv.ParseInt(string(v), 10, 64)
	return n
}

func (mpr *Provider[T]) load(context.Context) (*provider.Versioned[T], error) { //nolint:unparam // a cache.LoadFunc
	snapshot := mpr.loadLatest()
	return &snapshot, nil
}

func (mpr *Provider[T]) loadLatest() provider.Versioned[T] {
	state, currentVersion := mpr.raft.sc.documentState(mpr.codec.GetKey())

	mpr.raft.logger.Debug("Get metadata",
		slog.String("key", mpr.codec.GetKey()),
		slog.Any("metadata", state),
		slog.Any("current-version", currentVersion))
	if len(state) == 0 {
		return provider.Versioned[T]{
			Value:   mpr.codec.NewZero(),
			Version: toVersion(currentVersion),
		}
	}
	value, err := mpr.codec.UnmarshalJSON(state)
	if err != nil {
		// Committed metadata that cannot be decoded: failing to start is
		// safer than serving (or overwriting) a state we cannot read
		panic(err)
	}
	return provider.Versioned[T]{
		Value:   value,
		Version: toVersion(currentVersion),
	}
}

// write replicates the new state through raft, without any provider-side
// lock: the FSM applies entries serially and the expected-version check makes
// the update optimistic (ErrBadVersion on conflict).
func (mpr *Provider[T]) write(snapshot provider.Versioned[T]) (*provider.Versioned[T], error) {
	if err := mpr.raft.node.VerifyLeader().Error(); err != nil {
		return nil, err
	}

	data, err := mpr.codec.MarshalJSON(snapshot.Value)
	if err != nil {
		return nil, err
	}

	if mpr.raft.logger.Enabled(mpr.ctx, slog.LevelDebug) {
		_, currentVersion := mpr.raft.sc.documentState(mpr.codec.GetKey())
		mpr.raft.logger.Debug("Store into raft",
			slog.String("key", mpr.codec.GetKey()),
			slog.Any("metadata", data),
			slog.Any("expected-version", snapshot.Version),
			slog.Any("current-version", currentVersion))
	}

	cmd := raftOpCmd{
		Key:             mpr.codec.GetKey(),
		NewState:        json.RawMessage(data),
		ExpectedVersion: fromVersion(snapshot.Version),
	}

	serializedCmd, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	future := mpr.raft.node.Apply(serializedCmd, 30*time.Second)
	if err := future.Error(); err != nil {
		return nil, errors.Wrap(err, "failed to apply new cluster state")
	}

	applyRes, ok := future.Response().(*applyResult)
	if !ok {
		return nil, errors.New("failed to apply new cluster state")
	}
	if !applyRes.changeApplied {
		return nil, metadatacommon.ErrBadVersion
	}

	return &provider.Versioned[T]{
		Value:   mpr.codec.Clone(snapshot.Value),
		Version: toVersion(applyRes.newVersion),
	}, nil
}

func (mpr *Provider[T]) Load() *provider.Versioned[T] {
	return mpr.cache.Get()
}

func (mpr *Provider[T]) Subscribe() *cache.Subscription[provider.Versioned[T]] {
	return mpr.cache.Subscribe()
}

// Store writes the snapshot, then updates the cache: a failed write empties
// it, since the write may still have been applied. A load of the cache holds
// the cache lock until it stores its value, so a load that read the snapshot
// before the write cannot overwrite the cache update.
func (mpr *Provider[T]) Store(snapshot provider.Versioned[T]) (metadatacommon.Version, error) {
	stored, err := mpr.write(snapshot)
	if err != nil {
		mpr.cache.Invalidate()
		return metadatacommon.NotExists, err
	}
	mpr.cache.Set(stored)
	return stored.Version, nil
}
