// Copyright 2025 StreamNative, Inc.
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

package resources

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/emirpasic/gods/v2/trees/redblacktree"

	"github.com/oxia-db/oxia/common/process"

	"github.com/oxia-db/oxia/coordinator/model"
)

const (
	defaultLoadBalancerScheduleInterval = time.Second * 30
	defaultQuarantineTime               = time.Minute * 5
)

var DefaultLoadBalancerConfig = &model.LoadBalancer{
	ScheduleInterval: defaultLoadBalancerScheduleInterval,
	QuarantineTime:   defaultQuarantineTime,
}

type ClusterConfigResource interface {
	io.Closer

	Load() *model.ClusterConfig

	LoadLoadBalancer() *model.LoadBalancer

	Nodes() *linkedhashset.Set[string]

	NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata)

	NamespaceConfig(namespace string) (*model.NamespaceConfig, bool)

	Node(id string) (*model.Server, bool)
}

var _ ClusterConfigResource = &clusterConfig{}

type clusterConfig struct {
	*slog.Logger
	sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	clusterConfigProvider        func() (model.ClusterConfig, error)
	clusterConfigNotificationsCh chan any
	clusterConfigEventListener   ClusterConfigEventListener

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *model.ClusterConfig
	nodesIndex            *redblacktree.Tree[string, *model.Server]
	namespaceConfigsIndex *redblacktree.Tree[string, *model.NamespaceConfig]
}

func (ccf *clusterConfig) loadWithInitSlow() {
	ccf.clusterConfigLock.Lock()
	defer ccf.clusterConfigLock.Unlock()
	if ccf.currentClusterConfig != nil {
		return
	}
	_ = backoff.RetryNotify(func() error {
		newConfig, err := ccf.clusterConfigProvider()
		if err != nil {
			return err
		}
		ccf.currentClusterConfig = &newConfig
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		ccf.Warn(
			"failed to load cluster configuration, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	index := redblacktree.New[string, *model.Server]()
	for idx, server := range ccf.currentClusterConfig.Servers {
		index.Put(server.GetIdentifier(), &ccf.currentClusterConfig.Servers[idx])
	}
	ccf.nodesIndex = index
	ncIndex := redblacktree.New[string, *model.NamespaceConfig]()
	for idx, ns := range ccf.currentClusterConfig.Namespaces {
		ncIndex.Put(ns.Name, &ccf.currentClusterConfig.Namespaces[idx])
	}
	ccf.namespaceConfigsIndex = ncIndex
}

func (ccf *clusterConfig) Close() error {
	ccf.cancel()
	ccf.Wait()
	return nil
}

func (ccf *clusterConfig) Load() *model.ClusterConfig {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		ccf.loadWithInitSlow()
		ccf.clusterConfigLock.RLock()
	}
	return ccf.currentClusterConfig
}

func (ccf *clusterConfig) LoadLoadBalancer() *model.LoadBalancer {
	conf := ccf.Load()
	var balancer *model.LoadBalancer
	if balancer = conf.LoadBalancer; balancer == nil {
		balancer = DefaultLoadBalancerConfig
	}
	if balancer.QuarantineTime == 0 {
		balancer.QuarantineTime = defaultQuarantineTime
	}
	if balancer.ScheduleInterval == 0 {
		balancer.ScheduleInterval = defaultLoadBalancerScheduleInterval
	}
	return balancer
}

func (ccf *clusterConfig) Nodes() *linkedhashset.Set[string] {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		ccf.loadWithInitSlow()
		ccf.clusterConfigLock.RLock()
	}
	nodes := linkedhashset.New[string]()
	for idx := range ccf.currentClusterConfig.Servers {
		nodes.Add(ccf.currentClusterConfig.Servers[idx].GetIdentifier())
	}
	return nodes
}

func (ccf *clusterConfig) NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata) {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		ccf.loadWithInitSlow()
		ccf.clusterConfigLock.RLock()
	}
	nodes := linkedhashset.New[string]()
	for idx := range ccf.currentClusterConfig.Servers {
		nodes.Add(ccf.currentClusterConfig.Servers[idx].GetIdentifier())
	}
	metadata := ccf.currentClusterConfig.ServerMetadata
	return nodes, metadata
}

func (ccf *clusterConfig) NamespaceConfig(namespace string) (*model.NamespaceConfig, bool) {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		ccf.loadWithInitSlow()
		ccf.clusterConfigLock.RLock()
	}
	return ccf.namespaceConfigsIndex.Get(namespace)
}

func (ccf *clusterConfig) Node(id string) (*model.Server, bool) {
	ccf.clusterConfigLock.RLock()
	defer ccf.clusterConfigLock.RUnlock()
	if ccf.currentClusterConfig == nil {
		ccf.clusterConfigLock.RUnlock()
		ccf.loadWithInitSlow()
		ccf.clusterConfigLock.RLock()
	}
	return ccf.nodesIndex.Get(id)
}

func (ccf *clusterConfig) waitForUpdates() {
	defer ccf.Done()
	for {
		select {
		case <-ccf.ctx.Done():
			return

		case <-ccf.clusterConfigNotificationsCh:
			ccf.Info("Received cluster config change event")
			ccf.clusterConfigLock.Lock()
			oldClusterConfig := ccf.currentClusterConfig
			ccf.currentClusterConfig = nil
			ccf.clusterConfigLock.Unlock()

			ccf.loadWithInitSlow()

			ccf.clusterConfigLock.RLock()
			currentClusterConfig := ccf.currentClusterConfig
			ccf.clusterConfigLock.RUnlock()

			if reflect.DeepEqual(oldClusterConfig, currentClusterConfig) {
				ccf.Info("No cluster config changes detected")
				return
			}
			ccf.clusterConfigEventListener.ConfigChanged(currentClusterConfig)
		}
	}
}

func NewClusterConfigResource(ctx context.Context,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	clusterConfigEventListener ClusterConfigEventListener,
) ClusterConfigResource {
	ctx, cancelFunc := context.WithCancel(ctx)

	cc := &clusterConfig{
		Logger:                       slog.With("component", "cluster-config-resource"),
		WaitGroup:                    sync.WaitGroup{},
		ctx:                          ctx,
		cancel:                       cancelFunc,
		clusterConfigProvider:        clusterConfigProvider,
		clusterConfigNotificationsCh: clusterConfigNotificationsCh,
		clusterConfigEventListener:   clusterConfigEventListener,
	}

	if clusterConfigNotificationsCh != nil {
		cc.Add(1)
		go process.DoWithLabels(ctx, map[string]string{
			"component": "coordinator-action-worker",
		}, cc.waitForUpdates)
	}

	return cc
}
