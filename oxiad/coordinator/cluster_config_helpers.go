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

package coordinator

import (
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

const (
	defaultLoadBalancerScheduleInterval = 30 * time.Second
	defaultLoadBalancerQuarantineTime   = 5 * time.Minute
)

func (c *coordinator) clusterConfigCallbacks() controller.ClusterConfigCallbacks {
	return controller.ClusterConfigCallbacks{
		NamespaceConfig: c.namespaceConfig,
		Node:            c.node,
	}
}

func (c *coordinator) namespaceConfig(namespace string) (*model.NamespaceConfig, bool) {
	if c.clusterConfig == nil {
		return nil, false
	}
	for idx := range c.clusterConfig.Namespaces {
		if c.clusterConfig.Namespaces[idx].Name == namespace {
			return &c.clusterConfig.Namespaces[idx], true
		}
	}
	return nil, false
}

func (c *coordinator) node(id string) (*model.Server, bool) {
	if c.clusterConfig == nil {
		return nil, false
	}
	for idx := range c.clusterConfig.Servers {
		if c.clusterConfig.Servers[idx].GetIdentifier() == id {
			return &c.clusterConfig.Servers[idx], true
		}
	}
	return nil, false
}

func (c *coordinator) loadBalancerConfig() *model.LoadBalancer {
	config := &model.LoadBalancer{
		ScheduleInterval: defaultLoadBalancerScheduleInterval,
		QuarantineTime:   defaultLoadBalancerQuarantineTime,
	}
	if c.clusterConfig == nil || c.clusterConfig.LoadBalancer == nil {
		return config
	}
	config = &model.LoadBalancer{
		ScheduleInterval: c.clusterConfig.LoadBalancer.ScheduleInterval,
		QuarantineTime:   c.clusterConfig.LoadBalancer.QuarantineTime,
	}
	if config.ScheduleInterval == 0 {
		config.ScheduleInterval = defaultLoadBalancerScheduleInterval
	}
	if config.QuarantineTime == 0 {
		config.QuarantineTime = defaultLoadBalancerQuarantineTime
	}
	return config
}

func (c *coordinator) nodes() *linkedhashset.Set[string] {
	nodes := linkedhashset.New[string]()
	if c.clusterConfig == nil {
		return nodes
	}
	for _, server := range c.clusterConfig.Servers {
		nodes.Add(server.GetIdentifier())
	}
	return nodes
}

func (c *coordinator) nodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata) {
	nodes := c.nodes()
	if c.clusterConfig == nil {
		return nodes, map[string]model.ServerMetadata{}
	}
	return nodes, c.clusterConfig.ServerMetadata
}

func (c *coordinator) getDataServerInfo(id string) (*model.DataServerInfo, bool) {
	node, ok := c.node(id)
	if !ok {
		return nil, false
	}
	info := &model.DataServerInfo{Server: node}
	if metadata, ok := c.clusterConfig.ServerMetadata[id]; ok {
		info.Metadata = metadata
	}
	return info, true
}
