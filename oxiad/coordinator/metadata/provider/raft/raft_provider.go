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

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider[*commonproto.ClusterStatus] = (*Provider[*commonproto.ClusterStatus])(nil)
var _ provider.Provider[*commonproto.ClusterConfiguration] = (*Provider[*commonproto.ClusterConfiguration])(nil)

type Provider[T gproto.Message] struct {
	resourceType provider.ResourceType
	codec        provider.Codec[T]
	raft         *Raft
	ctx          context.Context
	ctxCancel    context.CancelFunc
	wg           sync.WaitGroup
	watchLock    sync.Mutex
	watch        *commonwatch.Watch[T]
	logger       *slog.Logger
}

func newProvider[T gproto.Message](r *Raft, resourceType provider.ResourceType, codec provider.Codec[T]) provider.Provider[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Provider[T]{
		resourceType: resourceType,
		codec:        codec,
		raft:         r,
		ctx:          ctx,
		ctxCancel:    cancel,
		logger: slog.With(
			slog.String("component", "metadata-provider-raft-watch"),
			slog.String("resource-type", string(resourceType)),
		),
	}
}

func NewProvider(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (provider.Provider[*commonproto.ClusterStatus], error) {
	metadataRaft, err := NewRaft(raftAddress, raftBootstrapNodes, raftDataDir)
	if err != nil {
		return nil, err
	}
	return newProvider(metadataRaft, provider.ResourceStatus, provider.ClusterStatusCodec), nil
}

func NewProviders(
	raftAddress string,
	raftBootstrapNodes []string,
	raftDataDir string,
) (statusProvider provider.Provider[*commonproto.ClusterStatus], configProvider provider.Provider[*commonproto.ClusterConfiguration], err error) {
	metadataRaft, err := NewRaft(raftAddress, raftBootstrapNodes, raftDataDir)
	if err != nil {
		return nil, nil, err
	}
	return newProvider(metadataRaft, provider.ResourceStatus, provider.ClusterStatusCodec),
		newProvider(metadataRaft, provider.ResourceConfig, provider.ClusterConfigCodec),
		nil
}

func (r *Raft) NewStatusProvider() provider.Provider[*commonproto.ClusterStatus] {
	return newProvider(r, provider.ResourceStatus, provider.ClusterStatusCodec)
}

func (r *Raft) NewConfigProvider() provider.Provider[*commonproto.ClusterConfiguration] {
	return newProvider(r, provider.ResourceConfig, provider.ClusterConfigCodec)
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

func toVersion(v int64) provider.Version {
	return provider.Version(strconv.FormatInt(v, 10))
}

func fromVersion(v provider.Version) int64 {
	n, _ := strconv.ParseInt(string(v), 10, 64)
	return n
}

func (mpr *Provider[T]) Get() (value T, version provider.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	document := mpr.raft.sc.document(mpr.resourceType)

	mpr.raft.logger.Debug("Get metadata",
		slog.String("resource-type", string(mpr.resourceType)),
		slog.Any("metadata", document.State),
		slog.Any("current-version", document.CurrentVersion))
	if len(document.State) == 0 {
		return value, toVersion(document.CurrentVersion), nil
	}
	value, err = mpr.codec.Unmarshal(document.State)
	return value, toVersion(document.CurrentVersion), err
}

func (mpr *Provider[T]) Store(value T, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	mpr.raft.Lock()
	defer mpr.raft.Unlock()

	if err = mpr.raft.node.VerifyLeader().Error(); err != nil {
		return provider.NotExists, err
	}

	data, err := mpr.codec.MarshalJSON(value)
	if err != nil {
		return provider.NotExists, err
	}

	mpr.raft.logger.Debug("Store into raft",
		slog.String("resource-type", string(mpr.resourceType)),
		slog.Any("metadata", data),
		slog.Any("expected-version", expectedVersion),
		slog.Any("current-version", mpr.raft.sc.document(mpr.resourceType).CurrentVersion))

	cmd := raftOpCmd{
		ResourceType:    mpr.resourceType,
		NewState:        json.RawMessage(data),
		ExpectedVersion: fromVersion(expectedVersion),
	}

	serializedCmd, err := json.Marshal(cmd)
	if err != nil {
		return provider.NotExists, err
	}

	future := mpr.raft.node.Apply(serializedCmd, 30*time.Second)
	if err := future.Error(); err != nil {
		return provider.NotExists, errors.Wrap(err, "failed to apply new cluster state")
	}

	applyRes, ok := future.Response().(*applyResult)
	if !ok {
		return provider.NotExists, errors.New("failed to apply new cluster state")
	}
	if !applyRes.changeApplied {
		panic(provider.ErrBadVersion)
	}

	return toVersion(applyRes.newVersion), nil
}

func (mpr *Provider[T]) Watch() (*commonwatch.Receiver[T], error) {
	mpr.watchLock.Lock()
	defer mpr.watchLock.Unlock()

	if mpr.watch == nil {
		changeReceiver := mpr.raft.changes.Subscribe()
		value, version, err := mpr.Get()
		if err != nil {
			return nil, err
		}
		if version == provider.NotExists {
			value = mpr.codec.NewZero()
		}
		mpr.watch = commonwatch.New(value)
		mpr.wg.Go(func() {
			mpr.watchLoop(changeReceiver)
		})
	}
	return mpr.watch.Subscribe(), nil
}

func (mpr *Provider[T]) watchLoop(changeReceiver *commonwatch.Receiver[raftStateChange]) {
	for {
		select {
		case <-mpr.ctx.Done():
			return
		case <-changeReceiver.Changed():
		}

		change := changeReceiver.Load()
		if change.ResourceType != mpr.resourceType {
			continue
		}

		value, _, err := mpr.Get()
		if err != nil {
			mpr.logger.Warn("Failed to load raft metadata after change", slog.Any("error", err))
			continue
		}
		mpr.watch.Publish(value)
	}
}
