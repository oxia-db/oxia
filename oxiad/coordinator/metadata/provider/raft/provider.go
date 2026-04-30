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
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	watch     *commonwatch.Watch[T]
	logger    *slog.Logger
}

func NewProvider[T gproto.Message](ctx context.Context, r *Raft, codec metadatacommon.Codec[T], watchEnabled metadatacommon.WatchMode) provider.Provider[T] {
	ctx, cancel := context.WithCancel(ctx)
	p := &Provider[T]{
		codec:     codec,
		raft:      r,
		ctx:       ctx,
		ctxCancel: cancel,
		logger: slog.With(
			slog.String("component", "metadata-provider-raft-watch"),
		),
	}

	value, version, err := p.Get()
	if err != nil {
		p.logger.Warn("Failed to load initial raft metadata", slog.Any("error", err))
		value = p.codec.NewZero()
	} else if version == metadatacommon.NotExists {
		value = p.codec.NewZero()
	}
	p.watch = commonwatch.New(value)
	return p
}

func (mpr *Provider[T]) OnApplied(key string, data []byte) {
	if mpr.codec.GetKey() != key {
		return
	}
	var entity T
	var err error
	if entity, err = mpr.codec.UnmarshalJSON(data); err != nil {
		mpr.logger.Warn("Failed to unmarshal data", slog.Any("error", err))
		return
	}
	mpr.watch.Publish(entity)
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

func (mpr *Provider[T]) Get() (value T, version metadatacommon.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	document := mpr.raft.sc.document(mpr.codec.GetKey())

	mpr.raft.logger.Debug("Get metadata",
		slog.String("key", mpr.codec.GetKey()),
		slog.Any("metadata", document.State),
		slog.Any("current-version", document.CurrentVersion))
	if len(document.State) == 0 {
		return value, toVersion(document.CurrentVersion), nil
	}
	value, err = mpr.codec.UnmarshalJSON(document.State)
	return value, toVersion(document.CurrentVersion), err
}

func (mpr *Provider[T]) Store(value T, expectedVersion metadatacommon.Version) (newVersion metadatacommon.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	if err = mpr.raft.node.VerifyLeader().Error(); err != nil {
		return metadatacommon.NotExists, err
	}

	data, err := mpr.codec.MarshalJSON(value)
	if err != nil {
		return metadatacommon.NotExists, err
	}

	mpr.raft.logger.Debug("Store into raft",
		slog.String("key", mpr.codec.GetKey()),
		slog.Any("metadata", data),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", mpr.raft.sc.document(mpr.codec.GetKey()).CurrentVersion))

	cmd := raftOpCmd{
		Key:             mpr.codec.GetKey(),
		NewState:        json.RawMessage(data),
		ExpectedVersion: fromVersion(expectedVersion),
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
		panic(metadatacommon.ErrBadVersion)
	}

	return toVersion(applyRes.newVersion), nil
}

func (mpr *Provider[T]) Watch() (*commonwatch.Receiver[T], error) {
	return mpr.watch.Subscribe(), nil
}
