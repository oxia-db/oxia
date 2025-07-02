// Copyright 2023 StreamNative, Inc.
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
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/coordinator/actions"

	"github.com/oxia-db/oxia/coordinator/controllers"
	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/coordinator/balancer"
	"github.com/oxia-db/oxia/coordinator/metadata"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/rpc"
	"github.com/oxia-db/oxia/coordinator/selectors"
	"github.com/oxia-db/oxia/coordinator/selectors/ensemble"
	"github.com/oxia-db/oxia/coordinator/utils"
	"github.com/oxia-db/oxia/proto"
)

type Coordinator interface {
	io.Closer
	controllers.ShardEventListener
	controllers.ShardAssignmentsProvider
	controllers.NodeEventListener
	resources.ClusterConfigEventListener

	NodeControllers() map[string]controllers.NodeController

	LoadBalancer() balancer.LoadBalancer

	StatusResource() resources.StatusResource
	ConfigResource() resources.ClusterConfigResource
}

var _ Coordinator = &coordinator{}

type coordinator struct {
	*slog.Logger
	sync.WaitGroup
	sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	statusResource resources.StatusResource
	configResource resources.ClusterConfigResource

	shardControllers map[int64]controllers.ShardController
	nodeControllers  map[string]controllers.NodeController
	// Draining nodes are nodes that were removed from the
	// nodes list. We keep sending them assignments updates
	// because they might be still reachable to clients.
	drainingNodes map[string]controllers.NodeController

	loadBalancer     balancer.LoadBalancer
	ensembleSelector selectors.Selector[*ensemble.Context, []string]

	clusterConfigChangeCh chan any

	assignmentsChanged concurrent.ConditionContext
	assignments        *proto.ShardAssignments

	rpc rpc.Provider
}

func (c *coordinator) LeaderElected(int64, model.Server, []model.Server) {
	c.Lock()
	defer c.Unlock()
	c.computeNewAssignments()
}

func (c *coordinator) ShardDeleted(int64) {
	c.Lock()
	defer c.Unlock()
	c.computeNewAssignments()
}

func (c *coordinator) LoadBalancer() balancer.LoadBalancer {
	return c.loadBalancer
}

func (c *coordinator) StatusResource() resources.StatusResource {
	return c.statusResource
}

func (c *coordinator) ConfigResource() resources.ClusterConfigResource {
	return c.configResource
}

func (c *coordinator) NodeControllers() map[string]controllers.NodeController {
	c.RLock()
	defer c.RUnlock()
	return c.nodeControllers
}

func (c *coordinator) ConfigChanged(newConfig *model.ClusterConfig) {
	c.Lock()
	defer c.Unlock()

	c.Info("Detected change in cluster config", slog.Any("newClusterConfig", newConfig))

	// Check for nodes to add
	for _, sa := range newConfig.Servers {
		if _, ok := c.nodeControllers[sa.GetIdentifier()]; ok {
			continue
		}
		// The node is present in the config, though we don't know it yet,
		// therefore it must be a newly added node
		c.Info("Detected new node", slog.Any("server", sa))
		if nc, ok := c.drainingNodes[sa.GetIdentifier()]; ok {
			// If there were any controller for a draining node, close it
			// and recreate it as a new node
			_ = nc.Close()
			delete(c.drainingNodes, sa.GetIdentifier())
		}
		c.nodeControllers[sa.GetIdentifier()] = controllers.NewNodeController(sa, c, c, c.rpc)
	}

	// Check for nodes to remove
	for serverID, nc := range c.nodeControllers {
		if _, exist := c.configResource.Node(serverID); exist {
			continue
		}
		c.Info("Detected a removed node", slog.Any("server", serverID))
		// Moved the node
		delete(c.nodeControllers, serverID)
		nc.SetStatus(controllers.Draining)
		c.drainingNodes[serverID] = nc
	}
	for _, sc := range c.shardControllers {
		sc.SyncServerAddress()
	}

	// compare and set
	currentStatus, version := c.statusResource.LoadWithVersion()
	var clusterStatus *model.ClusterStatus
	var shardsToAdd map[int64]string
	var shardsToDelete []int64
	for {
		clusterStatus, shardsToAdd, shardsToDelete = utils.ApplyClusterChanges(newConfig, currentStatus, c.selectNewEnsemble)
		if !c.statusResource.Swap(clusterStatus, version) {
			currentStatus, version = c.statusResource.LoadWithVersion()
			continue
		}
		break
	}
	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		if namespaceConfig, exist := c.configResource.NamespaceConfig(namespace); exist {
			c.shardControllers[shard] = controllers.NewShardController(namespace, shard, namespaceConfig, shardMetadata, c.configResource, c.statusResource, c, c.rpc)
			slog.Info("Added new shard", slog.Int64("shard", shard),
				slog.String("namespace", namespace), slog.Any("shard-metadata", shardMetadata))
		}
	}
	for _, shard := range shardsToDelete {
		if s, exist := c.shardControllers[shard]; exist {
			s.DeleteShard()
		}
	}

	c.computeNewAssignments()
	c.loadBalancer.Trigger()
}

func (c *coordinator) waitForAllNodesToBeAvailable() {
	c.Info("Waiting for all the nodes to be available")
	for {
		select {
		case <-time.After(1 * time.Second):
			c.Info("Start to check unavailable nodes")

			var unavailableNodes []string
			for nodeName, nc := range c.nodeControllers {
				if nc.Status() != controllers.Running {
					unavailableNodes = append(unavailableNodes, nodeName)
				}
			}
			if len(unavailableNodes) == 0 {
				c.Info("All nodes are now available")
				return
			}

			c.Info(
				"A part of nodes is not available",
				slog.Any("UnavailableNodeNames", unavailableNodes),
			)
		case <-c.ctx.Done():
			// Give up if we're closing the coordinator
			return
		}
	}
}

// selectNewEnsemble select a new server ensemble based on namespace policies and current cluster status.
// It uses the ensemble selector to choose appropriate servers and returns the selected server metadata or an error.
func (c *coordinator) selectNewEnsemble(ns *model.NamespaceConfig, editingStatus *model.ClusterStatus) ([]model.Server, error) {
	nodes, nodesMetadata := c.configResource.NodesWithMetadata()
	ensembleContext := &ensemble.Context{
		Candidates:         nodes,
		CandidatesMetadata: nodesMetadata,
		Policies:           ns.Policies,
		Status:             editingStatus,
		Replicas:           int(ns.ReplicationFactor),
		LoadRatioSupplier: func() *model.Ratio {
			groupedStatus, historyNodes := utils.GroupingShardsNodeByStatus(nodes, editingStatus)
			return c.loadBalancer.LoadRatioAlgorithm()(&model.RatioParams{NodeShardsInfos: groupedStatus, HistoryNodes: historyNodes})
		},
	}
	var ensembles []string
	var err error
	if ensembles, err = c.ensembleSelector.Select(ensembleContext); err != nil {
		return nil, err
	}
	esm := make([]model.Server, 0)
	for _, id := range ensembles {
		var node *model.Server
		var exist bool
		if node, exist = c.configResource.Node(id); !exist {
			return nil, fmt.Errorf("failed to find node %s", id)
		}
		esm = append(esm, *node)
	}
	return esm, nil
}

func (c *coordinator) Close() error {
	c.cancel()
	c.Wait()

	err := c.configResource.Close()
	for _, sc := range c.shardControllers {
		err = multierr.Append(err, sc.Close())
	}

	for _, nc := range c.nodeControllers {
		err = multierr.Append(err, nc.Close())
	}

	for _, nc := range c.drainingNodes {
		err = multierr.Append(err, nc.Close())
	}
	return err
}

func (c *coordinator) NodeBecameUnavailable(node model.Server) {
	c.Lock()
	if nc, ok := c.drainingNodes[node.GetIdentifier()]; ok {
		// The draining node became unavailable. Let's remove it
		delete(c.drainingNodes, node.GetIdentifier())
		_ = nc.Close()
	}

	ctrls := make(map[int64]controllers.ShardController)
	for k, sc := range c.shardControllers {
		ctrls[k] = sc
	}
	c.Unlock()

	for _, sc := range ctrls {
		sc.NodeBecameUnavailable(node)
	}
}

func (c *coordinator) WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	c.Lock()
	defer c.Unlock()

	for pb.Equal(currentValue, c.assignments) {
		// Wait on the condition until the assignments get changed
		if err := c.assignmentsChanged.Wait(ctx); err != nil {
			return nil, err
		}
	}

	return c.assignments, nil
}

func (c *coordinator) startBackgroundActionWorker() {
	defer c.Done()
	for {
		select {
		case ac := <-c.loadBalancer.Action():
			switch ac.Type() {
			case actions.SwapNode:
				c.handleActionSwap(ac)
			case actions.Election:
				c.handleActionElection(ac)
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *coordinator) handleActionElection(ac actions.Action) {
	var electionAc *actions.ElectionAction
	var ok bool
	if electionAc, ok = ac.(*actions.ElectionAction); !ok {
		panic("unexpected action type")
	}
	c.Info("Applying swap action", slog.Any("swap-action", ac))

	c.RLock()
	sc, ok := c.shardControllers[electionAc.Shard]
	c.RUnlock()
	if !ok {
		c.Warn("Shard controller not found", slog.Int64("shard", electionAc.Shard))
		electionAc.Done(nil)
		return
	}
	electionAc.Done(sc.Election(electionAc))
}

func (c *coordinator) handleActionSwap(ac actions.Action) {
	var swapAction *actions.SwapNodeAction
	var ok bool
	if swapAction, ok = ac.(*actions.SwapNodeAction); !ok {
		panic("unexpected action type")
	}
	defer swapAction.Done(nil)
	c.Info("Applying swap action", slog.Any("swap-action", ac))

	c.RLock()
	sc, ok := c.shardControllers[swapAction.Shard]
	c.RUnlock()
	if !ok {
		c.Warn("Shard controller not found", slog.Int64("shard", swapAction.Shard))
		return
	}

	if err := sc.SwapNode(swapAction.From, swapAction.To); err != nil {
		c.Warn("Failed to swap node", slog.Any("error", err), slog.Int64("shard", swapAction.Shard), slog.Any("swap-action", ac))
	}
}

// This is called while already holding the lock on the coordinator.
func (c *coordinator) computeNewAssignments() {
	c.assignments = &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{},
	}
	status := c.statusResource.Load()
	// Update the leader for the shards on all the namespaces
	for name, ns := range status.Namespaces {
		nsAssignments := &proto.NamespaceShardsAssignment{
			Assignments:    make([]*proto.ShardAssignment, 0),
			ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
		}

		for shard, a := range ns.Shards {
			var leader string
			if a.Leader != nil {
				leader = a.Leader.Public
			}
			if a.Status != model.ShardStatusDeleting {
				nsAssignments.Assignments = append(nsAssignments.Assignments,
					&proto.ShardAssignment{
						Shard:  shard,
						Leader: leader,
						ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
							Int32HashRange: &proto.Int32HashRange{
								MinHashInclusive: a.Int32HashRange.Min,
								MaxHashInclusive: a.Int32HashRange.Max,
							},
						},
					},
				)
			}
		}

		c.assignments.Namespaces[name] = nsAssignments
	}

	c.assignmentsChanged.Broadcast()
}

func NewCoordinator(meta metadata.Provider,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	rpcProvider rpc.Provider) (Coordinator, error) {
	c := &coordinator{
		Logger: slog.With(
			slog.String("component", "coordinator"),
		),
		WaitGroup:             sync.WaitGroup{},
		clusterConfigChangeCh: clusterConfigNotificationsCh,
		ensembleSelector:      ensemble.NewSelector(),
		shardControllers:      make(map[int64]controllers.ShardController),
		nodeControllers:       make(map[string]controllers.NodeController),
		drainingNodes:         make(map[string]controllers.NodeController),
		rpc:                   rpcProvider,
		statusResource:        resources.NewStatusResource(meta),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.configResource = resources.NewClusterConfigResource(c.ctx, clusterConfigProvider, clusterConfigNotificationsCh, c)
	c.assignmentsChanged = concurrent.NewConditionContext(c)

	c.loadBalancer = balancer.NewLoadBalancer(balancer.Options{
		Context:               c.ctx,
		StatusResource:        c.statusResource,
		ClusterConfigResource: c.configResource,
		NodeAvailableJudger: func(nodeID string) bool {
			c.RLock()
			defer c.RUnlock()
			nc := c.nodeControllers[nodeID]
			return nc.Status() == controllers.Running
		},
	})

	clusterConfig := c.configResource.Load()
	clusterStatus := c.statusResource.Load()

	// init node controllers
	for _, node := range clusterConfig.Servers {
		c.nodeControllers[node.GetIdentifier()] = controllers.NewNodeController(node, c, c, c.rpc)
	}

	// init status
	if clusterStatus == nil {
		// Before initializing the cluster, it's better to make sure we
		// have all the nodes available, otherwise the coordinator might be
		// the first component in getting started and will print out a lot
		// of error logs regarding failed leader elections
		c.waitForAllNodesToBeAvailable()
		c.Info("Performing initial assignment", slog.Any("clusterConfig", clusterConfig))

		clusterStatus, _, _ = utils.ApplyClusterChanges(clusterConfig, model.NewClusterStatus(), c.selectNewEnsemble)

		c.statusResource.Update(clusterStatus)
	} else {
		c.Info("Checking cluster config", slog.Any("clusterConfig", clusterConfig))

		var shardsToAdd map[int64]string
		var shardsToDelete []int64
		clusterStatus, shardsToAdd, shardsToDelete = utils.ApplyClusterChanges(clusterConfig,
			clusterStatus, c.selectNewEnsemble)

		if len(shardsToAdd) > 0 || len(shardsToDelete) > 0 {
			c.statusResource.Update(clusterStatus)
		}
	}

	// init shard controllers
	for ns, shards := range clusterStatus.Namespaces {
		for shard := range shards.Shards {
			shardMetadata := shards.Shards[shard]
			var nsConfig *model.NamespaceConfig
			var exist bool
			if nsConfig, exist = c.configResource.NamespaceConfig(ns); !exist {
				nsConfig = &model.NamespaceConfig{}
			}
			c.shardControllers[shard] = controllers.NewShardController(ns, shard, nsConfig, shardMetadata, c.configResource, c.statusResource, c, c.rpc)
		}
	}

	c.Add(1)
	go process.DoWithLabels(c.ctx, map[string]string{
		"component": "coordinator-action-worker",
	}, c.startBackgroundActionWorker)

	return c, nil
}
