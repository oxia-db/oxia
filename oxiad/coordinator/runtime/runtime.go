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

package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"

	"github.com/oxia-db/oxia/oxiad/common/sharding"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/ensemble"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/state"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

type runtime struct {
	sync.RWMutex

	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	insID     string

	metadata coordmetadata.Metadata

	shardControllers      map[int64]controller.ShardController
	splitControllers      map[int64]*controller.SplitController // keyed by parent shard ID
	dataServerControllers map[string]controller.DataServerController
	// Draining nodes are nodes that were removed from the
	// nodes list. We keep sending them assignments updates
	// because they might be still reachable to clients.
	drainingNodes map[string]controller.DataServerController

	loadBalancer     balancer.LoadBalancer
	ensembleSelector selector.Selector[*ensemble.Context, []string]

	assignmentsWatch *commonwatch.Watch[*proto.ShardAssignments]

	rpc rpc.Provider
}

func (c *runtime) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {
	c.Lock()
	defer c.Unlock()
	c.computeNewAssignments()
}

func (c *runtime) ShardDeleted(int64) {
	c.Lock()
	defer c.Unlock()
	c.computeNewAssignments()
}

func (c *runtime) LoadBalancer() balancer.LoadBalancer {
	return c.loadBalancer
}

func (c *runtime) Metadata() coordmetadata.Metadata {
	return c.metadata
}

func (c *runtime) ListDataServer() map[string]commonobject.Borrowed[*proto.DataServer] {
	c.RLock()
	defer c.RUnlock()
	dataServers := make(map[string]commonobject.Borrowed[*proto.DataServer], len(c.dataServerControllers))
	for name, dataServerController := range c.dataServerControllers {
		dataServers[name] = dataServerController.GetDataServer()
	}
	return dataServers
}

func (c *runtime) CreateDataServer(name string, dataServer *proto.DataServer) bool {
	c.Lock()
	defer c.Unlock()

	identity := dataServer.GetIdentity()
	if identity == nil {
		return false
	}
	if _, ok := c.dataServerControllers[name]; ok {
		return false
	}
	c.logger.Info("Detected new node", slog.Any("server", identity))
	if nc, ok := c.drainingNodes[name]; ok {
		_ = nc.Close()
		delete(c.drainingNodes, name)
	}
	c.dataServerControllers[name] = controller.NewDataServerController(
		c.ctx,
		dataServer,
		c,
		c,
		c.rpc,
		c.insID,
	)
	return true
}

func (c *runtime) DeleteDataServer(name string) {
	c.Lock()
	defer c.Unlock()

	nc, exist := c.dataServerControllers[name]
	if !exist {
		return
	}
	c.logger.Info("Detected a removed node", slog.Any("server", name))
	delete(c.dataServerControllers, name)
	nc.SetStatus(controller.Draining)
	c.drainingNodes[name] = nc
}

func (c *runtime) SyncShardControllerServerAddresses() {
	c.Lock()
	defer c.Unlock()

	for _, sc := range c.shardControllers {
		sc.SyncServerAddress()
	}
}

func (c *runtime) CreateNamespace(name string, namespaceConfig *proto.Namespace) bool {
	baseShardID := c.metadata.ReserveShardIDs(namespaceConfig.GetInitialShardCount())
	currentStatus := c.metadata.GetStatus().UnsafeBorrow()
	namespaceStatus := &proto.NamespaceStatus{
		Shards:            map[int64]*proto.ShardMetadata{},
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
	}
	status := &proto.ClusterStatus{
		Namespaces: make(map[string]*proto.NamespaceStatus, len(currentStatus.GetNamespaces())+1),
	}
	for name, existingNamespaceStatus := range currentStatus.GetNamespaces() {
		status.Namespaces[name] = existingNamespaceStatus
	}
	status.Namespaces[name] = namespaceStatus

	for _, shard := range sharding.GenerateShards(baseShardID, namespaceConfig.GetInitialShardCount()) {
		esm, err := c.selectNewEnsemble(name, shard.Id, namespaceConfig, status)
		if err != nil {
			c.logger.Error("failed to select new ensembles", slog.Any("shard", shard), slog.Any("error", err))
			continue
		}

		namespaceStatus.Shards[shard.Id] = &proto.ShardMetadata{
			Status:   proto.ShardStatusUnknown,
			Term:     -1,
			Leader:   nil,
			Ensemble: esm,
			Int32HashRange: &proto.HashRange{
				Min: shard.Min,
				Max: shard.Max,
			},
		}
	}

	created := c.metadata.CreateNamespaceStatus(name, namespaceStatus)
	if !created {
		return false
	}

	c.Lock()
	defer c.Unlock()

	for shard, shardMetadata := range namespaceStatus.GetShards() {
		c.shardControllers[shard] = controller.NewShardController(name, shard, namespaceConfig,
			shardMetadata, c.metadata, c.findDataServerFeatures,
			c, c.rpc, controller.DefaultPeriodicTasksInterval)
		slog.Info("Added new shard", slog.Int64("shard", shard),
			slog.String("namespace", name), slog.Any("shard-metadata", shardMetadata))
	}
	return true
}

func (c *runtime) DeleteNamespace(namespace string) {
	namespaceStatus := c.metadata.DeleteNamespaceStatus(namespace).UnsafeBorrow()
	if namespaceStatus == nil {
		return
	}
	c.logger.Info("Deleting namespace", slog.String("namespace", namespace), slog.Int("shards", len(namespaceStatus.GetShards())))
	c.Lock()
	defer c.Unlock()
	for shard := range namespaceStatus.GetShards() {
		if s, exist := c.shardControllers[shard]; exist {
			s.DeleteShard()
		}
	}
}

func (c *runtime) RecomputeAssignments() {
	c.Lock()
	defer c.Unlock()
	c.computeNewAssignments()
	c.loadBalancer.Trigger()
}

func (c *runtime) findDataServerFeatures(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature)
	for _, dataServer := range dataServers {
		dataServerID := dataServer.GetNameOrDefault()
		if serverController, exist := c.dataServerControllers[dataServerID]; exist {
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

func dataServersToCandidatesAndMetadata(dataServers map[string]commonobject.Borrowed[*proto.DataServer]) (
	*linkedhashset.Set[string],
	map[string]*proto.DataServerMetadata,
) {
	candidates := linkedhashset.New[string]()
	metadata := make(map[string]*proto.DataServerMetadata, len(dataServers))
	for name, borrowedDataServer := range dataServers {
		dataServer := borrowedDataServer.UnsafeBorrow()
		candidates.Add(name)
		if dataServer.GetMetadata() != nil {
			metadata[name] = dataServer.GetMetadata()
			continue
		}
		metadata[name] = &proto.DataServerMetadata{}
	}
	return candidates, metadata
}

// selectNewEnsemble select a new server ensemble based on namespace policy and current cluster status.
// It uses the ensemble selector to choose appropriate servers and returns the selected server metadata or an error.
func (c *runtime) selectNewEnsemble(namespace string, shard int64, ns *proto.Namespace, editingStatus *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
	dataServers := c.metadata.ListDataServer()
	nodes, metadata := dataServersToCandidatesAndMetadata(dataServers)
	ensembleContext := &ensemble.Context{
		Candidates:         nodes,
		CandidatesMetadata: metadata,
		AntiAffinities:     ns.GetAntiAffinities(),
		Status:             editingStatus,
		Namespace:          namespace,
		Shard:              shard,
		Replicas:           int(ns.GetReplicationFactor()),
		LoadRatioSupplier: func() *model.Ratio {
			groupedStatus, historyNodes := state.GroupingShardsNodeByStatus(nodes, editingStatus)
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
		borrowedDataServer, exist := dataServers[id]
		if !exist {
			return nil, fmt.Errorf("failed to find node %s", id)
		}
		dataServer := borrowedDataServer.UnsafeBorrow()
		if !exist || dataServer.GetIdentity() == nil {
			return nil, fmt.Errorf("failed to find node %s", id)
		}
		esm = append(esm, dataServer.GetIdentity())
	}
	return esm, nil
}

func (c *runtime) Close() error {
	c.ctxCancel()
	c.wg.Wait()

	var err error
	for _, sc := range c.splitControllers {
		sc.Close()
	}
	for _, sc := range c.shardControllers {
		err = multierr.Append(err, sc.Close())
	}

	for _, nc := range c.dataServerControllers {
		err = multierr.Append(err, nc.Close())
	}

	for _, nc := range c.drainingNodes {
		err = multierr.Append(err, nc.Close())
	}
	err = multierr.Append(err, c.rpc.Close())
	return err
}

func (c *runtime) BecameUnavailable(node *proto.DataServerIdentity) {
	c.Lock()
	if nc, ok := c.drainingNodes[node.GetNameOrDefault()]; ok {
		// The draining node became unavailable. Let's remove it
		delete(c.drainingNodes, node.GetNameOrDefault())
		go func() {
			// the callback will come from the node controller internal health check goroutine,
			// we should close it in the background goroutines to avoid any unexpected deadlock here
			if err := nc.Close(); err != nil {
				c.logger.Error("Failed to close node controller", slog.String("node", node.GetNameOrDefault()), slog.Any("error", err))
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

func (c *runtime) WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	receiver := c.assignmentsWatch.Subscribe()
	latest := c.assignmentsWatch.Load()
	if !pb.Equal(currentValue, latest) {
		return latest, nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-receiver.Changed():
		}

		latest = receiver.Load()
		if !pb.Equal(currentValue, latest) {
			return latest, nil
		}
	}
}

func (c *runtime) startBackgroundActionWorker() {
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

func (c *runtime) handleActionElection(ac action.Action) {
	var electionAc *action.ElectionAction
	var ok bool
	if electionAc, ok = ac.(*action.ElectionAction); !ok {
		panic("unexpected action type")
	}
	c.logger.Info("Applying swap action", slog.Any("swap-action", ac))

	c.RLock()
	sc, ok := c.shardControllers[electionAc.Shard]
	c.RUnlock()
	if !ok {
		c.logger.Warn("Shard controller not found", slog.Int64("shard", electionAc.Shard))
		electionAc.Done(nil)
		return
	}
	electionAc.Done(sc.Election(electionAc))
}

func (c *runtime) handleActionChangeEnsemble(ac action.Action) {
	var changeEnsembleAction *action.ChangeEnsembleAction
	var ok bool
	if changeEnsembleAction, ok = ac.(*action.ChangeEnsembleAction); !ok {
		panic("unexpected action type")
	}
	c.logger.Info("Applying swap action", slog.Any("swap-action", ac))

	c.RLock()
	sc, ok := c.shardControllers[changeEnsembleAction.Shard]
	c.RUnlock()
	if !ok {
		c.logger.Warn("Shard controller not found", slog.Int64("shard", changeEnsembleAction.Shard))
		return
	}

	sc.ChangeEnsemble(changeEnsembleAction)
}

// This is called while already holding the lock on the coordinator.
func (c *runtime) computeNewAssignments() {
	config := c.metadata.GetConfig().UnsafeBorrow()
	status := c.metadata.GetStatus().UnsafeBorrow()
	assignments := &proto.ShardAssignments{
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

		assignments.Namespaces[name] = nsAssignments
	}

	c.assignmentsWatch.Publish(assignments)
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

func dataServersFromStatus(status *proto.ClusterStatus) []*proto.DataServerIdentity {
	servers := make(map[string]*proto.DataServerIdentity)
	addServer := func(server *proto.DataServerIdentity) {
		if server == nil {
			return
		}
		servers[server.GetNameOrDefault()] = server
	}

	for _, namespace := range status.Namespaces {
		for _, shard := range namespace.Shards {
			addServer(shard.Leader)
			for _, server := range shard.Ensemble {
				addServer(server)
			}
			for _, server := range shard.RemovedNodes {
				addServer(server)
			}
			for _, server := range shard.PendingDeleteShardNodes {
				addServer(server)
			}
		}
	}

	result := make([]*proto.DataServerIdentity, 0, len(servers))
	for _, server := range servers {
		result = append(result, server)
	}
	return result
}

// InitiateSplit validates and initiates a shard split. It creates child shards
// in the cluster status and starts a SplitController to drive the split.
func (c *runtime) InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error) {
	c.Lock()
	defer c.Unlock()

	status := c.metadata.GetStatus().UnsafeBorrow()

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
	leftEnsemble, err := c.selectNewEnsemble(namespace, leftChildId, nsConfig, cloned)
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
	rightEnsemble, err := c.selectNewEnsemble(namespace, rightChildId, nsConfig, cloned)
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

	c.logger.Info("Split initiated",
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
			return c.selectNewEnsemble(ns, 0, c.namespaceConfigForSplit(ns), c.metadata.GetStatus().UnsafeBorrow())
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
func (c *runtime) SplitComplete(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()

	c.logger.Info("Split complete, triggering parent shard deletion",
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
		status := c.metadata.GetStatus().UnsafeBorrow()
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
func (c *runtime) SplitAborted(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()

	c.logger.Warn("Split aborted",
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

func (c *runtime) namespaceConfigForSplit(namespace string) *proto.Namespace {
	borrowedNsConfig, exist := c.metadata.GetNamespace(namespace)
	if !exist {
		return &proto.Namespace{}
	}
	return borrowedNsConfig.UnsafeBorrow()
}

// restartInProgressSplits checks the cluster status for any shards that have
// active SplitMetadata and creates SplitControllers to resume them.
func (c *runtime) restartInProgressSplits(clusterStatus *proto.ClusterStatus) {
	for ns, shards := range clusterStatus.Namespaces {
		for shardId, meta := range shards.Shards {
			if meta.Split == nil {
				continue
			}
			// Only create split controller from the parent shard (has ChildShardIds)
			if len(meta.Split.ChildShardIds) == 0 {
				continue
			}

			c.logger.Info("Resuming in-progress split",
				slog.String("namespace", ns),
				slog.Int64("parent-shard", shardId),
				slog.String("phase", meta.Split.GetPhaseOrDefault().String()),
			)

			sc := controller.NewSplitController(controller.SplitControllerConfig{
				Namespace:     ns,
				ParentShardId: shardId,
				Metadata:      c.metadata,
				RpcProvider:   c.rpc,
				EventListener: c,
				EnsembleSelector: func(namespace string) ([]*proto.DataServerIdentity, error) {
					return c.selectNewEnsemble(namespace, 0, c.namespaceConfigForSplit(namespace), c.metadata.GetStatus().UnsafeBorrow())
				},
			})
			c.splitControllers[shardId] = sc
		}
	}
}

func New(
	metadata coordmetadata.Metadata,
	rpcProvider rpc.ProviderFactory,
) (Runtime, error) {
	c := &runtime{
		logger: slog.With(
			slog.String("component", "coordinator"),
		),
		ensembleSelector:      ensemble.NewSelector(),
		shardControllers:      make(map[int64]controller.ShardController),
		splitControllers:      make(map[int64]*controller.SplitController),
		dataServerControllers: make(map[string]controller.DataServerController),
		drainingNodes:         make(map[string]controller.DataServerController),
		metadata:              metadata,
		assignmentsWatch:      commonwatch.New(&proto.ShardAssignments{}),
	}

	c.ctx, c.ctxCancel = context.WithCancel(context.Background())

	c.loadBalancer = balancer.NewLoadBalancer(balancer.Options{
		Context:  c.ctx,
		Metadata: c.metadata,
		NodeAvailableJudger: func(nodeID string) bool {
			c.RLock()
			defer c.RUnlock()
			nc := c.dataServerControllers[nodeID]
			return nc.Status() == controller.Running
		},
	})

	clusterStatus := c.metadata.GetStatus().UnsafeBorrow()
	c.insID = clusterStatus.InstanceId

	c.rpc = rpcProvider(c.insID)

	// init node controller
	for _, node := range dataServersFromStatus(clusterStatus) {
		dataServer := &proto.DataServer{Identity: node, Metadata: &proto.DataServerMetadata{}}
		c.dataServerControllers[node.GetNameOrDefault()] = controller.NewDataServerController(
			c.ctx,
			dataServer,
			c,
			c,
			c.rpc,
			c.insID,
		)
	}

	// init shard controller
	for ns, shards := range clusterStatus.Namespaces {
		for shard := range shards.Shards {
			shardMetadata := shards.Shards[shard]
			var nsConfig *proto.Namespace
			borrowedNsConfig, exist := c.metadata.GetNamespace(ns)
			if !exist {
				nsConfig = &proto.Namespace{}
			} else {
				nsConfig = borrowedNsConfig.UnsafeBorrow()
			}
			c.shardControllers[shard] = controller.NewShardController(ns, shard, nsConfig,
				shardMetadata, c.metadata, c.findDataServerFeatures,
				c, c.rpc, controller.DefaultPeriodicTasksInterval)
		}
	}

	// Restart any in-progress splits from persisted state
	c.restartInProgressSplits(clusterStatus)

	c.wg.Go(func() {
		process.DoWithLabels(c.ctx, map[string]string{
			"component": "coordinator-action-worker",
		}, c.startBackgroundActionWorker)
	})
	c.loadBalancer.Start()
	return c, nil
}
