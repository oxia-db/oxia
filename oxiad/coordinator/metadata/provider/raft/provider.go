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

	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Provider[T gproto.Message] struct {
	codec     metadatacommon.Codec[T]
	raft      *Raft
	watchMode metadatacommon.WatchMode
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	watch     *commonwatch.Watch[provider.Versioned[T]]
	logger    *slog.Logger
}

func NewProvider[T gproto.Message](ctx context.Context, r *Raft, codec metadatacommon.Codec[T], watchEnabled metadatacommon.WatchMode) provider.Provider[T] {
	ctx, cancel := context.WithCancel(ctx)
	p := &Provider[T]{
		codec:     codec,
		raft:      r,
		watchMode: watchEnabled,
		ctx:       ctx,
		ctxCancel: cancel,
		logger: slog.With(
			slog.String("component", "metadata-provider-raft-watch"),
		),
	}

	p.watch = commonwatch.New(p.loadLatest())
	return p
}

func (mpr *Provider[T]) OnApplied(key string, data []byte, version int64) {
	if mpr.codec.GetKey() != key {
		return
	}
	var entity T
	var err error
	if entity, err = mpr.codec.UnmarshalJSON(data); err != nil {
		mpr.logger.Warn("Failed to unmarshal data", slog.Any("error", err))
		return
	}
	mpr.watch.Publish(provider.Versioned[T]{
		Value:   entity,
		Version: toVersion(version),
	})
}

func (mpr *Provider[T]) WaitToBecomeLeader() error {
	<-mpr.raft.node.LeaderCh()
	return nil
}

func (mpr *Provider[T]) Close() error {
	mpr.ctxCancel()
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

func (mpr *Provider[T]) loadLatest() provider.Versioned[T] {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	document := mpr.raft.sc.document(mpr.codec.GetKey())

	mpr.raft.logger.Debug("Get metadata",
		slog.String("key", mpr.codec.GetKey()),
		slog.Any("metadata", document.State),
		slog.Any("current-version", document.CurrentVersion))
	if len(document.State) == 0 {
		return provider.Versioned[T]{
			Value:   mpr.codec.NewZero(),
			Version: toVersion(document.CurrentVersion),
		}
	}
	value, err := mpr.codec.UnmarshalJSON(document.State)
	if err != nil {
		panic(err)
	}
	return provider.Versioned[T]{
		Value:   value,
		Version: toVersion(document.CurrentVersion),
	}
}

func (mpr *Provider[T]) Store(snapshot provider.Versioned[T]) (newVersion metadatacommon.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	if err = mpr.raft.node.VerifyLeader().Error(); err != nil {
		return metadatacommon.NotExists, err
	}

	data, err := mpr.codec.MarshalJSON(snapshot.Value)
	if err != nil {
		return metadatacommon.NotExists, err
	}

	mpr.raft.logger.Debug("Store into raft",
		slog.String("key", mpr.codec.GetKey()),
		slog.Any("metadata", data),
		slog.Any("expected-version", snapshot.Version),
		slog.Any("current-version", mpr.raft.sc.document(mpr.codec.GetKey()).CurrentVersion))

	cmd := raftOpCmd{
		Key:             mpr.codec.GetKey(),
		NewState:        json.RawMessage(data),
		ExpectedVersion: fromVersion(snapshot.Version),
	}

	serializedCmd, err := json.Marshal(cmd)
	if err != nil {
		return metadatacommon.NotExists, err
	}

	future := mpr.raft.node.Apply(serializedCmd, 30*time.Second)
	if err := future.Error(); err != nil {
		return metadatacommon.NotExists, errors.Wrap(err, "failed to apply new cluster state")
	}

	applyRes, ok := future.Response().(*applyResult)
	if !ok {
		return metadatacommon.NotExists, errors.New("failed to apply new cluster state")
	}
	if !applyRes.changeApplied {
		return metadatacommon.NotExists, metadatacommon.ErrBadVersion
	}

	newVersion = toVersion(applyRes.newVersion)
	mpr.watch.Publish(provider.Versioned[T]{
		Value:   mpr.codec.Clone(snapshot.Value),
		Version: newVersion,
	})
	return newVersion, nil
}

func (mpr *Provider[T]) Watch() *commonwatch.Watch[provider.Versioned[T]] {
	return mpr.watch
}
