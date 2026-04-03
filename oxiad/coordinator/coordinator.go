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
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector/ensemble"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
)

type Coordinator interface {
	io.Closer
	ShardSplitter
	controller.ShardEventListener
	controller.ShardAssignmentsProvider
	controller.DataServerEventListener
	resource.ClusterConfigEventListener

	NodeControllers() map[string]controller.DataServerController

	LoadBalancer() balancer.LoadBalancer

	StatusResource() resource.StatusResource
	ConfigResource() resource.ClusterConfigResource
}

var _ Coordinator = &coordinator{}

type coordinator struct {
	*slog.Logger
	sync.WaitGroup
	sync.RWMutex

	// Cluster config resource wg, ConfigChanged method should be called after NewCoordinator finished.
	ccrWg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	statusResource resource.StatusResource
	configResource resource.ClusterConfigResource

	shardControllers map[int64]controller.ShardController
	splitControllers map[int64]*controller.SplitController // keyed by parent shard ID
	nodeControllers  map[string]controller.DataServerController
	// Draining nodes are nodes that were removed from the
	// nodes list. We keep sending them assignments updates
	// because they might be still reachable to clients.
	drainingNodes map[string]controller.DataServerController

	loadBalancer     balancer.LoadBalancer
	ensembleSelector selector.Selector[*ensemble.Context, []string]

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

func (c *coordinator) StatusResource() resource.StatusResource {
	return c.statusResource
}

func (c *coordinator) ConfigResource() resource.ClusterConfigResource {
	return c.configResource
}

func (c *coordinator) NodeControllers() map[string]controller.DataServerController {
	c.RLock()
	defer c.RUnlock()
	res := make(map[string]controller.DataServerController, len(c.nodeControllers))
	for k, v := range c.nodeControllers {
		res[k] = v
	}
	return res
}

func (c *coordinator) ConfigChanged(newConfig *model.ClusterConfig) {
	c.ccrWg.Wait()
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
		c.nodeControllers[sa.GetIdentifier()] = controller.NewDataServerController(c.ctx, sa, c, c, c.rpc)
	}

	// Check for nodes to remove
	for serverID, nc := range c.nodeControllers {
		if _, exist := c.configResource.Node(serverID); exist {
			continue
		}
		c.Info("Detected a removed node", slog.Any("server", serverID))
		// Moved the node
		delete(c.nodeControllers, serverID)
		nc.SetStatus(controller.Draining)
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
		clusterStatus, shardsToAdd, shardsToDelete = util.ApplyClusterChanges(newConfig, currentStatus, c.selectNewEnsemble)
		if !c.statusResource.Swap(clusterStatus, version) {
			currentStatus, version = c.statusResource.LoadWithVersion()
			continue
		}
		break
	}
	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		if namespaceConfig, exist := c.configResource.NamespaceConfig(namespace); exist {
			c.shardControllers[shard] = controller.NewShardController(namespace, shard, namespaceConfig,
				shardMetadata.Clone(), c.configResource, c.statusResource, c.findDataServerFeatures,
				c, c.rpc, controller.DefaultPeriodicTasksInterval)
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

func (c *coordinator) findDataServerFeatures(dataServers []model.Server) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature)
	for _, dataServer := range dataServers {
		dataServerID := dataServer.GetIdentifier()
		if serverController, exist := c.nodeControllers[dataServerID]; exist {
			features[dataServerID] = serverController.SupportedFeatures()
			continue
		}
		// fallback to draining node if alive not found
		if serverController, exist := c.drainingNodes[dataServerID]; exist {
			features[dataServerID] = serverController.SupportedFeatures()
			continue
		}
	}
	return features
}

func (c *coordinator) waitForAllNodesToBeAvailable() {
	c.Info("Waiting for all the nodes to be available")
	for {
		select {
		case <-time.After(1 * time.Second):
			c.Info("Start to check unavailable nodes")

			var unavailableNodes []string
			for nodeName, nc := range c.nodeControllers {
				if nc.Status() != controller.Running {
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

// selectNewEnsemble select a new server ensemble based on namespace policy and current cluster status.
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
			groupedStatus, historyNodes := util.GroupingShardsNodeByStatus(nodes, editingStatus)
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
	for _, sc := range c.splitControllers {
		sc.Close()
	}
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

func (c *coordinator) BecameUnavailable(node model.Server) {
	c.Lock()
	if nc, ok := c.drainingNodes[node.GetIdentifier()]; ok {
		// The draining node became unavailable. Let's remove it
		delete(c.drainingNodes, node.GetIdentifier())
		go func() {
			// the callback will come from the node controller internal health check goroutine,
			// we should close it in the background goroutines to avoid any unexpected deadlock here
			if err := nc.Close(); err != nil {
				c.Error("Failed to close node controller", slog.String("node", node.GetIdentifier()), slog.Any("error", err))
			}
		}()
	}

	ctrls := make(map[int64]controller.ShardController)
	for k, sc := range c.shardControllers {
		ctrls[k] = sc
	}
	c.Unlock()

	for _, sc := range ctrls {
		sc.BecameUnavailable(node)
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
			case action.SwapNode:
				c.handleActionChangeEnsemble(ac)
			case action.Election:
				c.handleActionElection(ac)
			default:
				panic("unknown action type")
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *coordinator) handleActionElection(ac action.Action) {
	var electionAc *action.ElectionAction
	var ok bool
	if electionAc, ok = ac.(*action.ElectionAction); !ok {
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

func (c *coordinator) handleActionChangeEnsemble(ac action.Action) {
	var changeEnsembleAction *action.ChangeEnsembleAction
	var ok bool
	if changeEnsembleAction, ok = ac.(*action.ChangeEnsembleAction); !ok {
		panic("unexpected action type")
	}
	c.Info("Applying swap action", slog.Any("swap-action", ac))

	c.RLock()
	sc, ok := c.shardControllers[changeEnsembleAction.Shard]
	c.RUnlock()
	if !ok {
		c.Warn("Shard controller not found", slog.Int64("shard", changeEnsembleAction.Shard))
		return
	}

	sc.ChangeEnsemble(changeEnsembleAction)
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
			// Skip shards that are deleting
			if a.Status == model.ShardStatusDeleting {
				continue
			}
			// Skip child shards that are still being split (child shards
			// have no ChildShardIDs, only a ParentShardId reference)
			if a.Split != nil && len(a.Split.ChildShardIDs) == 0 {
				continue
			}
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

		c.assignments.Namespaces[name] = nsAssignments
	}

	c.assignmentsChanged.Broadcast()
}

// InitiateSplit validates and initiates a shard split. It creates child shards
// in the cluster status and starts a SplitController to drive the split.
func (c *coordinator) InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error) {
	c.Lock()
	defer c.Unlock()

	status := c.statusResource.Load()

	// Validate namespace
	ns, exists := status.Namespaces[namespace]
	if !exists {
		return 0, 0, errors.Errorf("namespace %q not found", namespace)
	}

	// Validate parent shard
	parentMeta, exists := ns.Shards[parentShardId]
	if !exists {
		return 0, 0, errors.Errorf("shard %d not found in namespace %q", parentShardId, namespace)
	}
	if parentMeta.Status != model.ShardStatusSteadyState {
		return 0, 0, errors.Errorf("shard %d is not in steady state (status=%s)", parentShardId, parentMeta.Status)
	}
	if parentMeta.Split != nil {
		return 0, 0, errors.Errorf("shard %d already has an active split", parentShardId)
	}
	if len(parentMeta.PendingDeleteShardNodes) > 0 {
		return 0, 0, errors.Errorf("shard %d has pending ensemble changes", parentShardId)
	}
	if parentMeta.Int32HashRange.Max-parentMeta.Int32HashRange.Min < 1 {
		return 0, 0, errors.Errorf("shard %d hash range is too small to split", parentShardId)
	}

	// Compute split point
	var sp uint32
	if splitPoint != nil {
		sp = *splitPoint
		if sp < parentMeta.Int32HashRange.Min || sp >= parentMeta.Int32HashRange.Max {
			return 0, 0, errors.Errorf("split point %d is outside shard's hash range [%d, %d]",
				sp, parentMeta.Int32HashRange.Min, parentMeta.Int32HashRange.Max)
		}
	} else {
		sp = parentMeta.Int32HashRange.Min + (parentMeta.Int32HashRange.Max-parentMeta.Int32HashRange.Min)/2
	}

	// Allocate child shard IDs
	cloned := status.Clone()
	leftChildId := cloned.ShardIdGenerator
	rightChildId := cloned.ShardIdGenerator + 1
	cloned.ShardIdGenerator += 2

	// Select ensembles for children.
	// After selecting the left child's ensemble, insert it into the cloned
	// status so the right child's selection sees the updated load distribution
	// and picks a different server.
	nsConfig := c.namespaceConfigForSplit(namespace)
	leftEnsemble, err := c.selectNewEnsemble(nsConfig, cloned)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for left child")
	}

	// Update cloned status with left child placement before selecting right child
	cloned.Namespaces[namespace].Shards[leftChildId] = model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Ensemble: leftEnsemble,
		Int32HashRange: model.Int32HashRange{
			Min: parentMeta.Int32HashRange.Min,
			Max: sp,
		},
	}
	cloned.ServerIdx += nsConfig.ReplicationFactor

	rightEnsemble, err := c.selectNewEnsemble(nsConfig, cloned)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for right child")
	}

	nsCloned := cloned.Namespaces[namespace]

	// Create split metadata for parent
	parentMetaCloned := nsCloned.Shards[parentShardId]
	parentMetaCloned.Split = &model.SplitMetadata{
		Phase:         model.SplitPhaseBootstrap,
		ChildShardIDs: []int64{leftChildId, rightChildId},
		SplitPoint:    sp,
	}
	nsCloned.Shards[parentShardId] = parentMetaCloned

	// Create left child shard
	nsCloned.Shards[leftChildId] = model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Term:     0,
		Ensemble: leftEnsemble,
		Int32HashRange: model.Int32HashRange{
			Min: parentMeta.Int32HashRange.Min,
			Max: sp,
		},
		Split: &model.SplitMetadata{
			Phase:         model.SplitPhaseBootstrap,
			ParentShardId: parentShardId,
			SplitPoint:    sp,
		},
	}

	// Create right child shard
	nsCloned.Shards[rightChildId] = model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Term:     0,
		Ensemble: rightEnsemble,
		Int32HashRange: model.Int32HashRange{
			Min: sp + 1,
			Max: parentMeta.Int32HashRange.Max,
		},
		Split: &model.SplitMetadata{
			Phase:         model.SplitPhaseBootstrap,
			ParentShardId: parentShardId,
			SplitPoint:    sp,
		},
	}

	// Persist
	c.statusResource.Update(cloned)

	c.Info("Split initiated",
		slog.Int64("parent-shard", parentShardId),
		slog.Int64("left-child", leftChildId),
		slog.Int64("right-child", rightChildId),
		slog.Uint64("split-point", uint64(sp)),
	)

	// Create shard controllers for children
	for _, childId := range []int64{leftChildId, rightChildId} {
		childMeta := nsCloned.Shards[childId]
		c.shardControllers[childId] = controller.NewShardController(namespace, childId, nsConfig,
			childMeta, c.configResource, c.statusResource, c.findDataServerFeatures,
			c, c.rpc, controller.DefaultPeriodicTasksInterval)
	}

	// Start split controller
	sc := controller.NewSplitController(controller.SplitControllerConfig{
		Namespace:      namespace,
		ParentShardId:  parentShardId,
		StatusResource: c.statusResource,
		RpcProvider:    c.rpc,
		EventListener:  c,
		EnsembleSelector: func(ns string) ([]model.Server, error) {
			return c.selectNewEnsemble(c.namespaceConfigForSplit(ns), c.statusResource.Load())
		},
	})
	c.splitControllers[parentShardId] = sc

	return leftChildId, rightChildId, nil
}

// SplitComplete is called by the SplitController at the end of the Cutover
// phase, after children are re-elected in clean terms and the parent is marked
// Deleting. The coordinator triggers the parent shard's deletion (which retries
// indefinitely until all ensemble members have deleted the shard) and recomputes
// shard assignments so clients discover the children.
//
// NOTE: This is called from within the split controller's own goroutine,
// so we must NOT call sc.Close() on the split controller (that would deadlock
// on wg.Wait). Instead we just remove it from the map and let the goroutine
// finish naturally.
func (c *coordinator) SplitComplete(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()

	c.Info("Split complete, triggering parent shard deletion",
		slog.Int64("parent-shard", parentShard),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	// Remove split controller from map without calling Close (would deadlock).
	// The goroutine will return naturally after this callback.
	delete(c.splitControllers, parentShard)

	// Trigger the parent shard controller's deletion. The shard controller
	// retries DeleteShard RPCs indefinitely with backoff, handles unreachable
	// nodes, and removes the parent from cluster status when done.
	//
	// First, sync the shard controller's local metadata with the status
	// resource, since the split controller may have bumped the parent's term
	// during Cutover.
	if sc, exists := c.shardControllers[parentShard]; exists {
		// Sync shard controller metadata from the status resource.
		status := c.statusResource.Load()
		for _, ns := range status.Namespaces {
			if parentMeta, ok := ns.Shards[parentShard]; ok {
				sc.Metadata().Store(parentMeta)
				break
			}
		}
		sc.DeleteShard()
	}

	c.computeNewAssignments()
}

// SplitAborted is called by the SplitController when a split has been
// aborted due to timeout or cancellation. The split controller has already
// cleaned up observer cursors, deleted child shards from status, and
// cleared the parent's split metadata.
func (c *coordinator) SplitAborted(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()

	c.Warn("Split aborted",
		slog.Int64("parent-shard", parentShard),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	// Remove split controller from map (goroutine will return after this).
	delete(c.splitControllers, parentShard)

	// Close child shard controllers.
	for _, childId := range []int64{leftChild, rightChild} {
		if sc, exists := c.shardControllers[childId]; exists {
			_ = sc.Close()
			delete(c.shardControllers, childId)
		}
	}

	c.computeNewAssignments()
}

func (c *coordinator) namespaceConfigForSplit(namespace string) *model.NamespaceConfig {
	nsConfig, exist := c.configResource.NamespaceConfig(namespace)
	if !exist {
		nsConfig = &model.NamespaceConfig{}
	}
	return nsConfig
}

// restartInProgressSplits checks the cluster status for any shards that have
// active SplitMetadata and creates SplitControllers to resume them.
func (c *coordinator) restartInProgressSplits(clusterStatus *model.ClusterStatus) {
	for ns, shards := range clusterStatus.Namespaces {
		for shardId, meta := range shards.Shards {
			if meta.Split == nil {
				continue
			}
			// Only create split controller from the parent shard (has ChildShardIDs)
			if len(meta.Split.ChildShardIDs) == 0 {
				continue
			}

			c.Info("Resuming in-progress split",
				slog.String("namespace", ns),
				slog.Int64("parent-shard", shardId),
				slog.String("phase", meta.Split.Phase.String()),
			)

			sc := controller.NewSplitController(controller.SplitControllerConfig{
				Namespace:      ns,
				ParentShardId:  shardId,
				StatusResource: c.statusResource,
				RpcProvider:    c.rpc,
				EventListener:  c,
				EnsembleSelector: func(namespace string) ([]model.Server, error) {
					return c.selectNewEnsemble(c.namespaceConfigForSplit(namespace), c.statusResource.Load())
				},
			})
			c.splitControllers[shardId] = sc
		}
	}
}

func NewCoordinator(meta metadata.Provider,
	leaseWatch *concurrent.Watch[metadata.LeaseStatus],
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	rpcProvider rpc.Provider) (Coordinator, error) {
	c := &coordinator{
		Logger: slog.With(
			slog.String("component", "coordinator"),
		),
		WaitGroup:             sync.WaitGroup{},
		ccrWg:                 sync.WaitGroup{},
		clusterConfigChangeCh: clusterConfigNotificationsCh,
		ensembleSelector:      ensemble.NewSelector(),
		shardControllers:      make(map[int64]controller.ShardController),
		splitControllers:      make(map[int64]*controller.SplitController),
		nodeControllers:       make(map[string]controller.DataServerController),
		drainingNodes:         make(map[string]controller.DataServerController),
		rpc:                   rpcProvider,
	}
	c.ccrWg.Add(1)

	c.Info("This coordinator is now leader")

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.assignmentsChanged = concurrent.NewConditionContext(c)
	c.statusResource = resource.NewStatusResource(meta, leaseWatch)

	c.configResource = resource.NewClusterConfigResource(c.ctx, clusterConfigProvider, clusterConfigNotificationsCh, c)

	c.loadBalancer = balancer.NewLoadBalancer(balancer.Options{
		Context:               c.ctx,
		StatusResource:        c.statusResource,
		ClusterConfigResource: c.configResource,
		NodeAvailableJudger: func(nodeID string) bool {
			c.RLock()
			defer c.RUnlock()
			nc := c.nodeControllers[nodeID]
			return nc.Status() == controller.Running
		},
	})

	clusterConfig := c.configResource.Load()
	clusterStatus := c.statusResource.Load()

	// init node controller
	for _, node := range clusterConfig.Servers {
		c.nodeControllers[node.GetIdentifier()] = controller.NewDataServerController(c.ctx, node, c, c, c.rpc)
	}

	// init status
	if clusterStatus == nil {
		// Before initializing the cluster, it's better to make sure we
		// have all the nodes available, otherwise the coordinator might be
		// the first component in getting started and will print out a lot
		// of error logs regarding failed leader elections
		c.waitForAllNodesToBeAvailable()
		c.Info("Performing initial assignment", slog.Any("clusterConfig", clusterConfig))

		clusterStatus, _, _ = util.ApplyClusterChanges(clusterConfig, model.NewClusterStatus(), c.selectNewEnsemble)

		c.statusResource.Update(clusterStatus)
	} else {
		c.Info("Checking cluster config", slog.Any("clusterConfig", clusterConfig))

		var shardsToAdd map[int64]string
		var shardsToDelete []int64
		clusterStatus, shardsToAdd, shardsToDelete = util.ApplyClusterChanges(clusterConfig,
			clusterStatus, c.selectNewEnsemble)

		if len(shardsToAdd) > 0 || len(shardsToDelete) > 0 {
			c.statusResource.Update(clusterStatus)
		}
	}

	// init shard controller
	for ns, shards := range clusterStatus.Namespaces {
		for shard := range shards.Shards {
			shardMetadata := shards.Shards[shard]
			var nsConfig *model.NamespaceConfig
			var exist bool
			if nsConfig, exist = c.configResource.NamespaceConfig(ns); !exist {
				nsConfig = &model.NamespaceConfig{}
			}
			c.shardControllers[shard] = controller.NewShardController(ns, shard, nsConfig,
				shardMetadata, c.configResource, c.statusResource, c.findDataServerFeatures,
				c, c.rpc, controller.DefaultPeriodicTasksInterval)
		}
	}

	// Restart any in-progress splits from persisted state
	c.restartInProgressSplits(clusterStatus)

	c.Add(1)
	go process.DoWithLabels(c.ctx, map[string]string{
		"component": "coordinator-action-worker",
	}, c.startBackgroundActionWorker)

	c.loadBalancer.Start()
	c.ccrWg.Done()
	return c, nil
}
