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

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/ensemble"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
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

	NodeControllers() map[string]controller.DataServerController

	ReconcileClusterConfig(*proto.ClusterConfiguration) error

	LoadBalancer() balancer.LoadBalancer

	Metadata() coordmetadata.Metadata
}

var _ Coordinator = &coordinator{}

type coordinator struct {
	*slog.Logger
	sync.WaitGroup
	sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	insID  string

	metadata coordmetadata.Metadata

	shardControllers map[int64]controller.ShardController
	splitControllers map[int64]*controller.SplitController // keyed by parent shard ID
	nodeControllers  map[string]controller.DataServerController
	// Draining nodes are nodes that were removed from the
	// nodes list. We keep sending them assignments updates
	// because they might be still reachable to clients.
	drainingNodes map[string]controller.DataServerController

	loadBalancer     balancer.LoadBalancer
	ensembleSelector selector.Selector[*ensemble.Context, []string]

	assignmentsChanged concurrent.ConditionContext
	assignments        *proto.ShardAssignments

	rpc rpc.Provider
}

func (c *coordinator) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {
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

func (c *coordinator) Metadata() coordmetadata.Metadata {
	return c.metadata
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

func (c *coordinator) ReconcileClusterConfig(newConfig *proto.ClusterConfiguration) error {
	if !c.metadata.UpdateConfig(newConfig) {
		c.Info("No cluster config changes detected")
		return nil
	}
	c.ConfigChanged(newConfig)
	return nil
}

func (c *coordinator) ConfigChanged(newConfig *proto.ClusterConfiguration) {
	c.Lock()
	defer c.Unlock()

	c.Info("Detected change in cluster config", slog.Any("newClusterConfig", newConfig))

	// Check for nodes to add
	for _, sa := range newConfig.GetServers() {
		if _, ok := c.nodeControllers[sa.GetNameOrDefault()]; ok {
			continue
		}
		// The node is present in the config, though we don't know it yet,
		// therefore it must be a newly added node
		c.Info("Detected new node", slog.Any("server", sa))
		if nc, ok := c.drainingNodes[sa.GetNameOrDefault()]; ok {
			// If there were any controller for a draining node, close it
			// and recreate it as a new node
			_ = nc.Close()
			delete(c.drainingNodes, sa.GetNameOrDefault())
		}
		c.nodeControllers[sa.GetNameOrDefault()] = controller.NewDataServerController(
			c.ctx,
			sa,
			c,
			c,
			c.rpc,
			c.insID,
		)
	}

	// Check for nodes to remove
	for serverID, nc := range c.nodeControllers {
		if _, exist := c.metadata.Node(serverID); exist {
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

	clusterStatus, shardsToAdd, shardsToDelete := c.metadata.ApplyStatusChanges(newConfig, c.selectNewEnsemble)
	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		if namespaceConfig, exist := c.metadata.Namespace(namespace); exist {
			c.shardControllers[shard] = controller.NewShardController(namespace, shard, namespaceConfig,
				pb.Clone(shardMetadata).(*proto.ShardMetadata), c.metadata, c.findDataServerFeatures,
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

func (c *coordinator) findDataServerFeatures(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature)
	for _, dataServer := range dataServers {
		dataServerID := dataServer.GetNameOrDefault()
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

// selectNewEnsemble select a new server ensemble based on namespace policy and current cluster status.
// It uses the ensemble selector to choose appropriate servers and returns the selected server metadata or an error.
func (c *coordinator) selectNewEnsemble(ns *proto.Namespace, editingStatus *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
	nodes, nodesMetadata := c.metadata.NodesWithMetadata()
	ensembleContext := &ensemble.Context{
		Candidates:         nodes,
		CandidatesMetadata: nodesMetadata,
		HierarchyPolicies:  ns.GetPolicy(),
		Status:             editingStatus,
		Replicas:           int(ns.GetReplicationFactor()),
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
	esm := make([]*proto.DataServerIdentity, 0)
	for _, id := range ensembles {
		var node *proto.DataServerIdentity
		var exist bool
		if node, exist = c.metadata.Node(id); !exist {
			return nil, fmt.Errorf("failed to find node %s", id)
		}
		esm = append(esm, node)
	}
	return esm, nil
}

func (c *coordinator) Close() error {
	c.cancel()
	c.Wait()

	var err error
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
	err = multierr.Append(err, c.rpc.Close())
	return err
}

func (c *coordinator) BecameUnavailable(node *proto.DataServerIdentity) {
	c.Lock()
	if nc, ok := c.drainingNodes[node.GetNameOrDefault()]; ok {
		// The draining node became unavailable. Let's remove it
		delete(c.drainingNodes, node.GetNameOrDefault())
		go func() {
			// the callback will come from the node controller internal health check goroutine,
			// we should close it in the background goroutines to avoid any unexpected deadlock here
			if err := nc.Close(); err != nil {
				c.Error("Failed to close node controller", slog.String("node", node.GetNameOrDefault()), slog.Any("error", err))
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
	config := c.metadata.LoadConfig()
	status := c.metadata.LoadStatus()
	c.assignments = &proto.ShardAssignments{
		Namespaces:         map[string]*proto.NamespaceShardsAssignment{},
		AllowedAuthorities: mergedAuthorities(status, config.GetServers(), config.GetAllowExtraAuthorities()),
	}
	// Update the leader for the shards on all the namespaces
	for name, ns := range status.Namespaces {
		nsAssignments := &proto.NamespaceShardsAssignment{
			Assignments:    make([]*proto.ShardAssignment, 0),
			ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
		}

		for shard, a := range ns.Shards {
			var leader string
			if a.Leader != nil {
				leader = a.Leader.GetPublic()
			}
			// Skip shards that are deleting
			if a.GetStatusOrDefault() == proto.ShardStatusDeleting {
				continue
			}
			// Skip child shards that are still being split (child shards
			// have no ChildShardIds, only a ParentShardId reference)
			if a.Split != nil && len(a.Split.ChildShardIds) == 0 {
				continue
			}
			nsAssignments.Assignments = append(nsAssignments.Assignments,
				&proto.ShardAssignment{
					Shard:  shard,
					Leader: leader,
					ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
						Int32HashRange: &proto.Int32HashRange{
							MinHashInclusive: a.GetInt32HashRange().GetMin(),
							MaxHashInclusive: a.GetInt32HashRange().GetMax(),
						},
					},
				},
			)
		}

		c.assignments.Namespaces[name] = nsAssignments
	}

	c.assignmentsChanged.Broadcast()
}

func mergedAuthorities(status *proto.ClusterStatus, servers []*proto.DataServerIdentity, extraAuthorities []string) []string {
	authorities := linkedhashset.New[string]()
	addServerAuthorities := func(public string, internal string) {
		authorities.Add(public)
		authorities.Add(internal)
	}
	for _, server := range servers {
		addServerAuthorities(server.GetPublic(), server.GetInternal())
	}
	for _, namespace := range status.Namespaces {
		for _, shard := range namespace.Shards {
			for _, server := range shard.Ensemble {
				addServerAuthorities(server.GetPublic(), server.GetInternal())
			}
			for _, server := range shard.RemovedNodes {
				addServerAuthorities(server.GetPublic(), server.GetInternal())
			}
		}
	}
	for _, authority := range extraAuthorities {
		authorities.Add(authority)
	}
	return authorities.Values()
}

// InitiateSplit validates and initiates a shard split. It creates child shards
// in the cluster status and starts a SplitController to drive the split.
func (c *coordinator) InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error) {
	c.Lock()
	defer c.Unlock()

	status := c.metadata.LoadStatus()

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
	if parentMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return 0, 0, errors.Errorf("shard %d is not in steady state (status=%s)", parentShardId, parentMeta.GetStatus())
	}
	if parentMeta.Split != nil {
		return 0, 0, errors.Errorf("shard %d already has an active split", parentShardId)
	}
	if len(parentMeta.PendingDeleteShardNodes) > 0 {
		return 0, 0, errors.Errorf("shard %d has pending ensemble changes", parentShardId)
	}
	if parentMeta.GetInt32HashRange().GetMax()-parentMeta.GetInt32HashRange().GetMin() < 1 {
		return 0, 0, errors.Errorf("shard %d hash range is too small to split", parentShardId)
	}

	// Compute split point
	var sp uint32
	if splitPoint != nil {
		sp = *splitPoint
		if sp < parentMeta.GetInt32HashRange().GetMin() || sp >= parentMeta.GetInt32HashRange().GetMax() {
			return 0, 0, errors.Errorf("split point %d is outside shard's hash range [%d, %d]",
				sp, parentMeta.GetInt32HashRange().GetMin(), parentMeta.GetInt32HashRange().GetMax())
		}
	} else {
		sp = parentMeta.GetInt32HashRange().GetMin() + (parentMeta.GetInt32HashRange().GetMax()-parentMeta.GetInt32HashRange().GetMin())/2
	}

	// Allocate child shard IDs
	cloned := pb.Clone(status).(*proto.ClusterStatus) //nolint:revive
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
	cloned.Namespaces[namespace].Shards[leftChildId] = &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Ensemble: leftEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: parentMeta.GetInt32HashRange().GetMin(),
			Max: sp,
		},
	}
	cloned.ServerIdx += nsConfig.GetReplicationFactor()

	rightEnsemble, err := c.selectNewEnsemble(nsConfig, cloned)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for right child")
	}

	nsCloned := cloned.Namespaces[namespace]

	// Create split metadata for parent
	parentMetaCloned := nsCloned.Shards[parentShardId]
	parentMetaCloned.Split = &proto.SplitMetadata{
		Phase:         proto.SplitPhaseBootstrap,
		ChildShardIds: []int64{leftChildId, rightChildId},
		SplitPoint:    sp,
	}

	// Create left child shard
	nsCloned.Shards[leftChildId] = &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     0,
		Ensemble: leftEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: parentMeta.GetInt32HashRange().GetMin(),
			Max: sp,
		},
		Split: &proto.SplitMetadata{
			Phase:         proto.SplitPhaseBootstrap,
			ParentShardId: parentShardId,
			SplitPoint:    sp,
		},
	}

	// Create right child shard
	nsCloned.Shards[rightChildId] = &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     0,
		Ensemble: rightEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: sp + 1,
			Max: parentMeta.GetInt32HashRange().GetMax(),
		},
		Split: &proto.SplitMetadata{
			Phase:         proto.SplitPhaseBootstrap,
			ParentShardId: parentShardId,
			SplitPoint:    sp,
		},
	}

	// Persist
	c.metadata.UpdateStatus(cloned)

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
			childMeta, c.metadata, c.findDataServerFeatures,
			c, c.rpc, controller.DefaultPeriodicTasksInterval)
	}

	// Start split controller
	sc := controller.NewSplitController(controller.SplitControllerConfig{
		Namespace:     namespace,
		ParentShardId: parentShardId,
		Metadata:      c.metadata,
		RpcProvider:   c.rpc,
		EventListener: c,
		EnsembleSelector: func(ns string) ([]*proto.DataServerIdentity, error) {
			return c.selectNewEnsemble(c.namespaceConfigForSplit(ns), c.metadata.LoadStatus())
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
		status := c.metadata.LoadStatus()
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

func (c *coordinator) namespaceConfigForSplit(namespace string) *proto.Namespace {
	nsConfig, exist := c.metadata.Namespace(namespace)
	if !exist {
		nsConfig = &proto.Namespace{}
	}
	return nsConfig
}

// restartInProgressSplits checks the cluster status for any shards that have
// active SplitMetadata and creates SplitControllers to resume them.
func (c *coordinator) restartInProgressSplits(clusterStatus *proto.ClusterStatus) {
	for ns, shards := range clusterStatus.Namespaces {
		for shardId, meta := range shards.Shards {
			if meta.Split == nil {
				continue
			}
			// Only create split controller from the parent shard (has ChildShardIds)
			if len(meta.Split.ChildShardIds) == 0 {
				continue
			}

			c.Info("Resuming in-progress split",
				slog.String("namespace", ns),
				slog.Int64("parent-shard", shardId),
				slog.String("phase", meta.Split.GetPhaseOrDefault()),
			)

			sc := controller.NewSplitController(controller.SplitControllerConfig{
				Namespace:     ns,
				ParentShardId: shardId,
				Metadata:      c.metadata,
				RpcProvider:   c.rpc,
				EventListener: c,
				EnsembleSelector: func(namespace string) ([]*proto.DataServerIdentity, error) {
					return c.selectNewEnsemble(c.namespaceConfigForSplit(namespace), c.metadata.LoadStatus())
				},
			})
			c.splitControllers[shardId] = sc
		}
	}
}

func newCoordinator(
	metadata coordmetadata.Metadata,
	rpcProvider rpc.ProviderFactory,
) (Coordinator, error) {
	c := &coordinator{
		Logger: slog.With(
			slog.String("component", "coordinator"),
		),
		WaitGroup:        sync.WaitGroup{},
		ensembleSelector: ensemble.NewSelector(),
		shardControllers: make(map[int64]controller.ShardController),
		splitControllers: make(map[int64]*controller.SplitController),
		nodeControllers:  make(map[string]controller.DataServerController),
		drainingNodes:    make(map[string]controller.DataServerController),
		metadata:         metadata,
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.assignmentsChanged = concurrent.NewConditionContext(c)

	c.loadBalancer = balancer.NewLoadBalancer(balancer.Options{
		Context:  c.ctx,
		Metadata: c.metadata,
		NodeAvailableJudger: func(nodeID string) bool {
			c.RLock()
			defer c.RUnlock()
			nc := c.nodeControllers[nodeID]
			return nc.Status() == controller.Running
		},
	})

	clusterConfig := c.metadata.LoadConfig()
	clusterStatus := c.metadata.LoadStatus()
	c.insID = clusterStatus.InstanceId

	c.rpc = rpcProvider(c.insID)

	// init node controller
	for _, node := range clusterConfig.GetServers() {
		c.nodeControllers[node.GetNameOrDefault()] = controller.NewDataServerController(
			c.ctx,
			node,
			c,
			c,
			c.rpc,
			c.insID,
		)
	}
	c.Info("Checking cluster config", slog.Any("clusterConfig", clusterConfig))
	clusterStatus, _, _ = c.metadata.ApplyStatusChanges(clusterConfig, c.selectNewEnsemble)

	// init shard controller
	for ns, shards := range clusterStatus.Namespaces {
		for shard := range shards.Shards {
			shardMetadata := shards.Shards[shard]
			var nsConfig *proto.Namespace
			var exist bool
			if nsConfig, exist = c.metadata.Namespace(ns); !exist {
				nsConfig = &proto.Namespace{}
			}
			c.shardControllers[shard] = controller.NewShardController(ns, shard, nsConfig,
				shardMetadata, c.metadata, c.findDataServerFeatures,
				c, c.rpc, controller.DefaultPeriodicTasksInterval)
		}
	}

	// Restart any in-progress splits from persisted state
	c.restartInProgressSplits(clusterStatus)

	c.Go(func() {
		process.DoWithLabels(c.ctx, map[string]string{
			"component": "coordinator-action-worker",
		}, c.startBackgroundActionWorker)
	})
	c.loadBalancer.Start()
	reconciler.New(c.ctx, metadata, c)
	return c, nil
}

func NewCoordinator(
	metadata coordmetadata.Metadata,
	rpcProvider rpc.ProviderFactory,
) (Coordinator, error) {
	return newCoordinator(metadata, rpcProvider)
}
