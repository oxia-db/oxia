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

package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"

	"github.com/oxia-db/oxia/oxiad/common/sharding"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/autosplit"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/ensemble"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/state"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/dataserver"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/shard"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

// dataServerRecoveryStabilizationWindow is how long a data server must stay
// Running after recovering from a health-check failure before the load
// balancer considers it for leader rebalancing again. Failure handling (the
// re-election of leaders away from a dead node) is not affected.
const dataServerRecoveryStabilizationWindow = 5 * time.Minute

type runtime struct {
	sync.RWMutex

	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	insID     string

	// closed is set (under the write lock) when Close starts, so that late
	// shard-controller callbacks become no-ops instead of racing with the
	// teardown (metadata may be closed right after Close returns).
	closed bool
	// callbacksWg tracks in-flight callbacks that close shard controllers
	// outside the runtime lock.
	callbacksWg sync.WaitGroup

	metadata coordmetadata.Metadata

	shardControllers      map[int64]shardcontroller.Controller
	dataServerControllers map[string]dataservercontroller.Controller
	// Draining nodes are nodes that were removed from the
	// nodes list. We keep sending them assignments updates
	// because they might be still reachable to clients.
	drainingNodes map[string]dataservercontroller.Controller

	loadBalancer     balancer.LoadBalancer
	autoSplitMonitor *autosplit.Monitor
	ensembleSelector selector.Selector[*ensemble.Context, []string]

	assignmentsWatch *commonwatch.Watch[*proto.ShardAssignments]

	rpc rpc.Provider
}

func (c *runtime) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {
	c.Lock()
	defer c.Unlock()
	c.computeNewAssignments()
}

func (c *runtime) ShardDeleted(shard int64) {
	c.Lock()
	if c.closed {
		// Close tears down all the controllers from its own snapshot of the
		// maps, including this shard's.
		c.Unlock()
		return
	}
	c.callbacksWg.Add(1)
	defer c.callbacksWg.Done()

	sc, exists := c.shardControllers[shard]
	if exists {
		delete(c.shardControllers, shard)
	}
	c.computeNewAssignments()
	c.Unlock()

	if exists {
		if err := sc.Close(); err != nil {
			c.logger.Error("Failed to close shard controller", slog.Int64("shard", shard), slog.Any("error", err))
		}
	}
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

func (c *runtime) ListDataServerStatus() map[string]*proto.DataServerStatus {
	c.RLock()
	defer c.RUnlock()

	statuses := make(map[string]*proto.DataServerStatus, len(c.dataServerControllers)+len(c.drainingNodes))
	for name, dataServerController := range c.dataServerControllers {
		statuses[name] = &proto.DataServerStatus{
			State:             dataServerController.Status().ToProto(),
			SupportedFeatures: dataServerController.SupportedFeatures(),
		}
	}
	for name, dataServerController := range c.drainingNodes {
		statuses[name] = &proto.DataServerStatus{
			State:             dataServerController.Status().ToProto(),
			SupportedFeatures: dataServerController.SupportedFeatures(),
		}
	}
	return statuses
}

func (c *runtime) GetDataServerStatus(name string) (*proto.DataServerStatus, bool) {
	c.RLock()
	defer c.RUnlock()

	dataServerController, found := c.dataServerControllers[name]
	if !found {
		dataServerController, found = c.drainingNodes[name]
	}
	if !found {
		return nil, false
	}
	return &proto.DataServerStatus{
		State:             dataServerController.Status().ToProto(),
		SupportedFeatures: dataServerController.SupportedFeatures(),
	}, true
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
	c.dataServerControllers[name] = dataservercontroller.NewController(
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
	nc.SetStatus(dataservercontroller.Draining)
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
	status := c.metadata.ListNamespaceStatus()
	namespaceStatus := &proto.NamespaceStatus{
		Shards:            map[int64]*proto.ShardMetadata{},
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
	}
	status[name] = commonobject.Borrow(namespaceStatus)

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
		c.shardControllers[shard] = shardcontroller.NewController(name, shard, namespaceConfig,
			shardMetadata, c.metadata, c.findDataServerFeatures,
			c, shardcontroller.SplitterConfig{
				EnsembleSelector: c.selectSplitEnsemble,
				EventListener:    c,
			}, c.rpc, shardcontroller.DefaultPeriodicTasksInterval)
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
	c.RLock()
	defer c.RUnlock()

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
func (c *runtime) selectNewEnsemble(namespace string, shard int64, ns *proto.Namespace, editingStatus map[string]commonobject.Borrowed[*proto.NamespaceStatus]) ([]*proto.DataServerIdentity, error) {
	dataServers := c.metadata.ListDataServer()
	nodes, metadata := dataServersToCandidatesAndMetadata(dataServers)
	ensembleContext := &ensemble.Context{
		Candidates:         nodes,
		CandidatesMetadata: metadata,
		AntiAffinities:     ns.GetAntiAffinities(),
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

func (c *runtime) selectSplitEnsemble(
	namespace string,
	shard int64,
	editingStatus map[string]commonobject.Borrowed[*proto.NamespaceStatus],
) ([]*proto.DataServerIdentity, error) {
	return c.selectNewEnsemble(namespace, shard, c.namespaceConfigForSplit(namespace), editingStatus)
}

func (c *runtime) Close() error {
	c.ctxCancel()

	if c.autoSplitMonitor != nil {
		c.autoSplitMonitor.Close()
	}

	// Snapshot the controller maps under the lock: shard-controller callbacks
	// (e.g. ShardDeleted, running on a detached goroutine) mutate them
	// concurrently. The controllers must still be closed outside the lock,
	// because their callbacks acquire it.
	c.Lock()
	c.closed = true
	shardControllers := maps.Clone(c.shardControllers)
	dataServerControllers := maps.Clone(c.dataServerControllers)
	drainingNodes := maps.Clone(c.drainingNodes)
	c.Unlock()

	// The shard controllers must be closed before waiting for the action
	// worker: the worker blocks on in-flight election actions, which only
	// complete once the shard controllers' retry loops get canceled
	var err error
	for _, sc := range shardControllers {
		err = multierr.Append(err, sc.Close())
	}

	// Wait for callbacks that are still closing shard controllers removed from
	// the map. Controller Close is idempotent, so overlapping with the loop
	// above is safe.
	c.callbacksWg.Wait()

	c.wg.Wait()

	for _, nc := range dataServerControllers {
		err = multierr.Append(err, nc.Close())
	}

	for _, nc := range drainingNodes {
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

	ctrls := make(map[int64]shardcontroller.Controller)
	for k, sc := range c.shardControllers {
		ctrls[k] = sc
	}
	c.Unlock()

	for _, sc := range ctrls {
		sc.BecameUnavailable(node)
	}
}

func (c *runtime) SubscribeShardAssignments() *commonwatch.Receiver[*proto.ShardAssignments] {
	return c.assignmentsWatch.Subscribe()
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
		electionAc.Done("")
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
		changeEnsembleAction.Error(constant.ErrResourceUnavailable)
		return
	}

	sc.ChangeEnsemble(changeEnsembleAction)
}

// This is called while already holding the lock on the coordinator.
func (c *runtime) computeNewAssignments() {
	config := c.metadata.GetConfig().UnsafeBorrow()
	status := c.metadata.ListNamespaceStatus()
	assignments := &proto.ShardAssignments{
		Namespaces:         map[string]*proto.NamespaceShardsAssignment{},
		AllowedAuthorities: mergedAuthorities(status, config.GetServers(), config.GetAllowExtraAuthorities()),
	}
	// Update the leader for the shards on all the namespaces
	for name, borrowedNs := range status {
		ns := borrowedNs.UnsafeBorrow()
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

func mergedAuthorities(status map[string]commonobject.Borrowed[*proto.NamespaceStatus], servers []*proto.DataServerIdentity, extraAuthorities []string) []string {
	authorities := linkedhashset.New[string]()
	addServerAuthorities := func(public string, internal string) {
		authorities.Add(oxiadcommonrpc.StripAuthorityScheme(public))
		authorities.Add(oxiadcommonrpc.StripAuthorityScheme(internal))
	}
	for _, server := range servers {
		addServerAuthorities(server.GetPublic(), server.GetInternal())
	}
	for _, borrowedNamespace := range status {
		namespace := borrowedNamespace.UnsafeBorrow()
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
		authorities.Add(oxiadcommonrpc.StripAuthorityScheme(authority))
	}
	return authorities.Values()
}

func dataServersFromStatus(status map[string]commonobject.Borrowed[*proto.NamespaceStatus]) []*proto.DataServerIdentity {
	servers := make(map[string]*proto.DataServerIdentity)
	addServer := func(server *proto.DataServerIdentity) {
		if server == nil {
			return
		}
		servers[server.GetNameOrDefault()] = server
	}

	for _, borrowedNamespace := range status {
		namespace := borrowedNamespace.UnsafeBorrow()
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

// InitiateSplit delegates split initiation to the parent shard controller. The
// controller executes the action on its event-loop thread, serialized with
// elections and other shard metadata transitions.
func (c *runtime) InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error) {
	if _, exists := c.metadata.GetShardStatus(namespace, parentShardId); !exists {
		return 0, 0, errors.Errorf("shard %d not found in namespace %q", parentShardId, namespace)
	}

	c.RLock()
	sc, exists := c.shardControllers[parentShardId]
	c.RUnlock()
	if !exists {
		return 0, 0, errors.Errorf("shard %d not found in namespace %q", parentShardId, namespace)
	}

	result, err := sc.Split(action.NewSplitAction(parentShardId, splitPoint))
	if err != nil {
		return 0, 0, err
	}
	return result.LeftChild, result.RightChild, nil
}

// SplitStarted creates child shard controllers after the parent controller
// persists the initial split metadata. The parent controller owns and starts
// the remaining split state machine.
func (c *runtime) SplitStarted(namespace string, parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return
	}

	if _, parentExists := c.shardControllers[parentShard]; !parentExists {
		c.logger.Error("Split parent controller not found", slog.Int64("parent-shard", parentShard))
		return
	}

	nsConfig := c.namespaceConfigForSplit(namespace)
	for _, childID := range []int64{leftChild, rightChild} {
		childMeta, exists := c.metadata.GetShardStatus(namespace, childID)
		if !exists {
			c.logger.Error("Split child metadata not found", slog.Int64("child-shard", childID))
			continue
		}
		c.shardControllers[childID] = shardcontroller.NewController(
			namespace,
			childID,
			nsConfig,
			childMeta.UnsafeBorrow(),
			c.metadata,
			c.findDataServerFeatures,
			c,
			shardcontroller.SplitterConfig{
				EnsembleSelector: c.selectSplitEnsemble,
				EventListener:    c,
			},
			c.rpc,
			shardcontroller.DefaultPeriodicTasksInterval,
		)
	}
}

// SplitComplete is called by the parent shard controller at the end of Cutover
// phase, after children are re-elected in clean terms and the parent is marked
// Deleting. The coordinator triggers the parent shard's deletion (which retries
// indefinitely until all ensemble members have deleted the shard) and recomputes
// shard assignments so clients discover the children.
func (c *runtime) SplitComplete(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return
	}

	c.logger.Info("Split complete, triggering parent shard deletion",
		slog.Int64("parent-shard", parentShard),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	// Trigger the parent shard controller's deletion. The shard controller
	// retries DeleteShard RPCs indefinitely with backoff, handles unreachable
	// nodes, and removes the parent from cluster status when done.
	if sc, exists := c.shardControllers[parentShard]; exists {
		sc.DeleteShard()
	}

	c.computeNewAssignments()
}

// SplitAborted is called by the parent shard controller when a split has been
// aborted due to timeout or cancellation. The shard controller has already
// cleaned up observer cursors, deleted child shards from status, and
// cleared the parent's split metadata.
func (c *runtime) SplitAborted(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.callbacksWg.Add(1)
	defer c.callbacksWg.Done()

	c.logger.Warn("Split aborted",
		slog.Int64("parent-shard", parentShard),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	childControllers := make([]shardcontroller.Controller, 0, 2)
	for _, childId := range []int64{leftChild, rightChild} {
		if sc, exists := c.shardControllers[childId]; exists {
			childControllers = append(childControllers, sc)
			delete(c.shardControllers, childId)
		}
	}

	c.computeNewAssignments()
	c.Unlock()

	// Controller shutdown can invoke runtime callbacks, so it must not run
	// while holding the runtime lock.
	for _, sc := range childControllers {
		_ = sc.Close()
	}
}

func (c *runtime) namespaceConfigForSplit(namespace string) *proto.Namespace {
	borrowedNsConfig, exist := c.metadata.GetNamespace(namespace)
	if !exist {
		return &proto.Namespace{}
	}
	return borrowedNsConfig.UnsafeBorrow()
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
		shardControllers:      make(map[int64]shardcontroller.Controller),
		dataServerControllers: make(map[string]dataservercontroller.Controller),
		drainingNodes:         make(map[string]dataservercontroller.Controller),
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
			nc, ok := c.dataServerControllers[nodeID]
			if !ok {
				// The controller might not have been created yet for a
				// data server that was just registered
				return false
			}
			// A node that just recovered from a health-check failure is left
			// out of leader rebalancing until it has been stable for a while:
			// moving leaders straight back onto it is what turns one flap
			// into a rebalancing loop.
			return nc.IsStablyRunning(dataServerRecoveryStabilizationWindow)
		},
	})

	clusterStatus := c.metadata.ListNamespaceStatus()
	c.insID = c.metadata.GetInstanceID()

	c.rpc = rpcProvider(c.insID)

	// init node controller
	for _, node := range dataServersFromStatus(clusterStatus) {
		dataServer := &proto.DataServer{Identity: node, Metadata: &proto.DataServerMetadata{}}
		c.dataServerControllers[node.GetNameOrDefault()] = dataservercontroller.NewController(
			c.ctx,
			dataServer,
			c,
			c,
			c.rpc,
			c.insID,
		)
	}

	// init shard controller
	for ns, borrowedShards := range clusterStatus {
		shards := borrowedShards.UnsafeBorrow()
		for shard := range shards.Shards {
			shardMetadata := shards.Shards[shard]
			var nsConfig *proto.Namespace
			borrowedNsConfig, exist := c.metadata.GetNamespace(ns)
			if !exist {
				nsConfig = &proto.Namespace{}
			} else {
				nsConfig = borrowedNsConfig.UnsafeBorrow()
			}
			c.shardControllers[shard] = shardcontroller.NewController(ns, shard, nsConfig,
				shardMetadata, c.metadata, c.findDataServerFeatures,
				c, shardcontroller.SplitterConfig{
					EnsembleSelector: c.selectSplitEnsemble,
					EventListener:    c,
				}, c.rpc, shardcontroller.DefaultPeriodicTasksInterval)
		}
	}

	c.wg.Go(func() {
		process.DoWithLabels(c.ctx, map[string]string{
			"component": "coordinator-action-worker",
		}, c.startBackgroundActionWorker)
	})
	c.loadBalancer.Start()

	autoSplitInterval := c.metadata.GetConfig().UnsafeBorrow().GetAutoSplitWithDefaults().GetCollectionIntervalDurationOrDefault()
	c.autoSplitMonitor = autosplit.NewMonitor(c.metadata, c.rpc, c, autoSplitInterval)
	c.autoSplitMonitor.Start()

	return c, nil
}
