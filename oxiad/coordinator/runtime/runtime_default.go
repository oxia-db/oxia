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

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/oxiad/common/health"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"

	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/common/sharding"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/autosplit"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/ensemble"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/state"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/dataserver"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/shard"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

type runtime struct {
	sync.RWMutex

	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	// wg tracks the action worker, the in-flight ShardDeleted callbacks and
	// the closes of the drained data server controllers. The callbacks add
	// to it only under the lock while not closed, so none races with Close.
	wg    sync.WaitGroup
	insID string

	// closed is set (under the write lock) when Close starts, so that late
	// shard-controller callbacks become no-ops instead of racing with the
	// teardown (metadata may be closed right after Close returns).
	closed bool

	metadata coordmetadata.Metadata

	shardControllers map[int64]shardcontroller.Controller
	splitControllers map[int64]*shardcontroller.SplitController // keyed by parent shard ID
	// dataServerControllers keeps the controller of a data server removed
	// from the configuration until it is stored DRAINED: it keeps sending
	// the assignments, which clients may still ask it for.
	dataServerControllers map[string]dataservercontroller.Controller

	loadBalancer     balancer.LoadBalancer
	autoSplitMonitor *autosplit.Monitor
	ensembleSelector selector.Selector[*ensemble.Context, []string]

	assignmentsWatch *commonwatch.Watch[*proto.ShardAssignments]

	rpc rpc.ControllerRPCProvider
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
	c.wg.Add(1)
	defer c.wg.Done()

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

func (c *runtime) ListLiveDataServer() map[string]commonobject.Borrowed[*proto.DataServer] {
	c.RLock()
	defer c.RUnlock()
	dataServers := make(map[string]commonobject.Borrowed[*proto.DataServer], len(c.dataServerControllers))
	for name, dataServerController := range c.dataServerControllers {
		if !dataserver.IsRemoved(dataServerController.GetStatus().GetState()) {
			dataServers[name] = dataServerController.GetDataServer()
		}
	}
	return dataServers
}

func (c *runtime) ListLiveDataServerStatus() map[string]*proto.DataServerStatus {
	c.RLock()
	defer c.RUnlock()

	statuses := make(map[string]*proto.DataServerStatus, len(c.dataServerControllers))
	for name, dataServerController := range c.dataServerControllers {
		statuses[name] = &proto.DataServerStatus{
			State:             dataServerController.GetStatus().GetState(),
			SupportedFeatures: dataServerController.GetFeatures(),
		}
	}
	return statuses
}

func (c *runtime) GetLiveDataServerStatus(name string) (*proto.DataServerStatus, bool) {
	c.RLock()
	defer c.RUnlock()

	dataServerController, found := c.dataServerControllers[name]
	if !found {
		return nil, false
	}
	return &proto.DataServerStatus{
		State:             dataServerController.GetStatus().GetState(),
		SupportedFeatures: dataServerController.GetFeatures(),
	}, true
}

func (c *runtime) CreateDataServer(name string, dataServer *proto.DataServer) bool {
	identity := dataServer.GetIdentity()
	if identity == nil {
		return false
	}

	c.Lock()
	defer c.Unlock()
	if _, ok := c.dataServerControllers[name]; ok {
		return false
	}
	c.logger.Info("Detected new node", slog.Any("server", identity))
	c.dataServerControllers[name] = dataservercontroller.NewController(
		c.ctx,
		dataServer,
		c.metadata,
		c,
		c,
		c.rpc,
		c.insID,
		oxiatime.SlowInitialInterval,
		health.DefaultCheckPolicy,
	)
	return true
}

func (c *runtime) DeleteDataServer(name string) {
	c.Lock()
	defer c.Unlock()

	nc, exist := c.dataServerControllers[name]
	if !exist || dataserver.IsRemoved(nc.GetStatus().GetState()) {
		return
	}
	c.logger.Info("Detected a removed node", slog.Any("server", name))
	nc.Drain()
}

func (c *runtime) RefreshShardControllers() {
	c.Lock()
	defer c.Unlock()

	for _, sc := range c.shardControllers {
		sc.Refresh()
	}
}

func (c *runtime) CreateNamespace(name string, namespaceConfig *proto.Namespace) error {
	_, exists, err := c.metadata.GetNamespaceStatus(name)
	if err != nil {
		return err
	}
	if exists {
		return c.initShardControllers(name, namespaceConfig)
	}

	baseShardID, err := c.metadata.AllocateShardIDs(namespaceConfig.GetInitialShardCount())
	if err != nil {
		return err
	}
	status, err := c.metadata.ListNamespaceStatus()
	if err != nil {
		return err
	}
	namespaceStatus := &proto.NamespaceStatus{
		Shards:            map[int64]*proto.ShardMetadata{},
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
	}
	status[name] = commonobject.Borrow(namespaceStatus)

	for _, shard := range sharding.GenerateShards(baseShardID, namespaceConfig.GetInitialShardCount()) {
		c.RLock()
		esm, err := c.selectNewEnsemble(name, shard.Id, namespaceConfig, status, nil)
		c.RUnlock()
		if err != nil {
			return errors.Wrapf(err, "failed to select the ensemble of shard %d", shard.Id)
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

	if err := c.metadata.CreateNamespaceStatus(name, namespaceStatus); err != nil {
		return err
	}

	return c.initShardControllers(name, namespaceConfig)
}

func (c *runtime) initShardControllers(name string, namespaceConfig *proto.Namespace) error {
	c.Lock()
	defer c.Unlock()

	// Shard deletion removes status before removing its controller from the
	// map. Read under the runtime lock to avoid reviving a deleted shard.
	namespaceStatus, exists, err := c.metadata.GetNamespaceStatus(name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: namespace %q status disappeared during controller initialization",
			errs.ErrConflict, name)
	}
	for shard, shardMetadata := range namespaceStatus.UnsafeBorrow().GetShards() {
		if shardMetadata.GetStatusOrDefault() == proto.ShardStatusDeleting {
			continue
		}
		if _, exists := c.shardControllers[shard]; exists {
			continue
		}
		c.shardControllers[shard] = shardcontroller.NewController(name, shard, namespaceConfig,
			shardMetadata, c.metadata, c.findDataServerFeatures,
			c, c.rpc, shardcontroller.DefaultPeriodicTasksInterval)
		slog.Info("Added new shard", slog.Int64("shard", shard),
			slog.String("namespace", name), slog.Any("shard-metadata", shardMetadata))
	}
	return nil
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
			s.Delete()
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
	return c.findDataServerFeaturesLocked(dataServers)
}

// findDataServerFeaturesLocked is findDataServerFeatures, for the callers that
// already hold the lock.
func (c *runtime) findDataServerFeaturesLocked(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature)
	for _, dataServer := range dataServers {
		dataServerID := dataServer.GetNameOrDefault()
		if serverController, exist := c.dataServerControllers[dataServerID]; exist {
			features[dataServerID] = serverController.GetFeatures()
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

func cloneNamespaceStatuses(namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus]) map[string]commonobject.Borrowed[*proto.NamespaceStatus] {
	statuses := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(namespaces))
	for namespace, status := range namespaces {
		statuses[namespace] = commonobject.Borrow(pb.Clone(status.UnsafeBorrow()).(*proto.NamespaceStatus))
	}
	return statuses
}

// selectNewEnsemble select a new server ensemble based on namespace policy and current cluster status.
// It uses the ensemble selector to choose appropriate servers and returns the selected server metadata or an error.
// When eligible is set, only the data servers that it accepts can be selected.
// It must be called with the lock held.
func (c *runtime) selectNewEnsemble(namespace string, shard int64, ns *proto.Namespace, editingStatus map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	eligible func(dataServer *proto.DataServerIdentity) bool) ([]*proto.DataServerIdentity, error) {
	dataServers, err := c.metadata.ListDataServer()
	if err != nil {
		return nil, err
	}
	// Only a live data server takes shards: one that left has no controller,
	// or a draining one, also when the configuration has it again
	for name := range dataServers {
		if nc, live := c.dataServerControllers[name]; !live || dataserver.IsRemoved(nc.GetStatus().GetState()) {
			delete(dataServers, name)
		}
	}
	nodes, metadata := dataServersToCandidatesAndMetadata(dataServers)
	candidates := nodes
	if eligible != nil {
		candidates = nodes.Select(func(_ int, name string) bool {
			return eligible(dataServers[name].UnsafeBorrow().GetIdentity())
		})
	}
	ensembleContext := &ensemble.Context{
		Candidates:         candidates,
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
	if ensembles, err = c.ensembleSelector.Select(ensembleContext); err != nil {
		return nil, err
	}
	esm := make([]*proto.DataServerIdentity, 0)
	for _, id := range ensembles {
		borrowedDataServer, exist := dataServers[id]
		if !exist {
			return nil, fmt.Errorf("failed to find node %s", id)
		}
		esm = append(esm, borrowedDataServer.UnsafeBorrow().GetIdentity())
	}
	return esm, nil
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
	splitControllers := maps.Clone(c.splitControllers)
	shardControllers := maps.Clone(c.shardControllers)
	dataServerControllers := maps.Clone(c.dataServerControllers)
	c.Unlock()

	// The shard controllers must be closed before waiting for the action
	// worker: the worker blocks on in-flight election actions, which only
	// complete once the shard controllers' retry loops get canceled
	var err error
	for _, sc := range splitControllers {
		sc.Close()
	}
	for _, sc := range shardControllers {
		err = multierr.Append(err, sc.Close())
	}

	// Also waits for any in-flight ShardDeleted callback: it may still be
	// closing the shard controller it removed from the map. Controller Close
	// is idempotent, so overlapping with the loop above is safe.
	c.wg.Wait()

	for _, nc := range dataServerControllers {
		err = multierr.Append(err, nc.Close())
	}

	err = multierr.Append(err, c.rpc.Close())
	return err
}

func (c *runtime) BecameUnavailable(node *proto.DataServerIdentity) {
	c.RLock()
	ctrls := maps.Clone(c.shardControllers)
	c.RUnlock()

	for _, sc := range ctrls {
		sc.BecameUnavailable(node)
	}
}

func (c *runtime) BecameDrained(node *proto.DataServerIdentity) {
	c.Lock()
	defer c.Unlock()
	// A controller created since for the name, when the data server was
	// added again, isn't DRAINED
	nc, ok := c.dataServerControllers[node.GetNameOrDefault()]
	// Once closed, the runtime closes the controller itself
	if c.closed || !ok || nc.GetStatus().GetState() != proto.DataServerState_DATA_SERVER_STATE_DRAINED {
		return
	}
	delete(c.dataServerControllers, node.GetNameOrDefault())
	// The callback comes from a goroutine of the controller, which Close waits for
	c.wg.Go(func() {
		if err := nc.Close(); err != nil {
			c.logger.Error("Failed to close node controller", slog.String("node", node.GetNameOrDefault()), slog.Any("error", err))
		}
	})
}

func (c *runtime) FeaturesDiscovered(node *proto.DataServerIdentity) {
	c.RLock()
	defer c.RUnlock()

	for _, sc := range c.shardControllers {
		sc.FeaturesDiscovered(node)
	}
}

func (c *runtime) SubscribeShardAssignments() *commonwatch.Receiver[*proto.ShardAssignments] {
	return c.assignmentsWatch.Subscribe()
}

func (c *runtime) startBackgroundActionWorker() {
	for {
		select {
		case ac := <-c.loadBalancer.Action():
			c.dispatchAction(ac)

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *runtime) dispatchAction(ac action.Action) {
	c.logger.Info("Applying swap action", slog.Any("swap-action", ac))

	c.RLock()
	sc, ok := c.shardControllers[ac.GetShard()]
	c.RUnlock()
	if !ok {
		c.logger.Warn("Shard controller not found", slog.Int64("shard", ac.GetShard()))
		ac.Error(constant.ErrResourceUnavailable)
		return
	}
	sc.Dispatch(ac)
}

// This is called while already holding the lock on the coordinator.
func (c *runtime) computeNewAssignments() {
	_ = backoff.RetryNotify(c.computeNewAssignments0, oxiatime.NewBackOff(c.ctx), func(err error, retryAfter time.Duration) {
		c.logger.Warn("Failed to compute the shard assignments, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", retryAfter))
	})
}

func (c *runtime) computeNewAssignments0() error {
	borrowedConfig, err := c.metadata.GetConfig()
	if err != nil {
		return err
	}
	config := borrowedConfig.UnsafeBorrow()
	status, err := c.metadata.ListNamespaceStatus()
	if err != nil {
		return err
	}
	namespaces, err := c.metadata.ListNamespace()
	if err != nil {
		return err
	}
	assignments := &proto.ShardAssignments{
		Namespaces:         map[string]*proto.NamespaceShardsAssignment{},
		AllowedAuthorities: mergedAuthorities(status, config.GetServers(), config.GetAllowExtraAuthorities()),
	}
	// Update the leader for the shards on all the namespaces
	for name, borrowedNs := range status {
		ns := borrowedNs.UnsafeBorrow()
		keySorting, _ := namespaces[name].UnsafeBorrow().GetKeySortingType()
		nsAssignments := &proto.NamespaceShardsAssignment{
			Assignments:    make([]*proto.ShardAssignment, 0),
			ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			KeySorting:     keySorting.ToKeySorting(),
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
	return nil
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
			if change := shard.GetEnsembleChange(); change != nil {
				addServerAuthorities(change.GetFrom().GetPublic(), change.GetFrom().GetInternal())
			}
		}
	}
	for _, authority := range extraAuthorities {
		authorities.Add(oxiadcommonrpc.StripAuthorityScheme(authority))
	}
	return authorities.Values()
}

// New starts a runtime whose controllers stop when ctx is cancelled or the
// runtime is closed.
func New(
	ctx context.Context,
	metadata coordmetadata.Metadata,
	rpcProvider rpc.ControllerRPCProviderFactory,
) (Runtime, error) {
	clusterStatus, err := metadata.ListNamespaceStatus()
	if err != nil {
		return nil, err
	}
	dataServerStatus, err := metadata.ListDataServerStatus()
	if err != nil {
		return nil, err
	}
	dataServers := dataserver.GetStatusDataServers(clusterStatus, dataServerStatus)
	insID, err := metadata.GetInstanceID()
	if err != nil {
		return nil, err
	}
	config, err := metadata.GetConfig()
	if err != nil {
		return nil, err
	}
	loadBalancerConfig, err := metadata.GetLoadBalancerConfig()
	if err != nil {
		return nil, err
	}
	namespaces := make(map[string]*proto.Namespace, len(config.UnsafeBorrow().GetNamespaces()))
	for _, namespace := range config.UnsafeBorrow().GetNamespaces() {
		namespaces[namespace.GetName()] = namespace
	}

	c := &runtime{
		logger: slog.With(
			slog.String("component", "coordinator"),
		),
		ensembleSelector:      ensemble.NewSelector(),
		shardControllers:      make(map[int64]shardcontroller.Controller),
		splitControllers:      make(map[int64]*shardcontroller.SplitController),
		dataServerControllers: make(map[string]dataservercontroller.Controller),
		metadata:              metadata,
		insID:                 insID,
		assignmentsWatch:      commonwatch.New[*proto.ShardAssignments](nil),
	}

	c.ctx, c.ctxCancel = context.WithCancel(ctx)

	c.loadBalancer = balancer.NewLoadBalancer(balancer.Options{
		Context:            c.ctx,
		Metadata:           c.metadata,
		LoadBalancerConfig: loadBalancerConfig.UnsafeBorrow(),
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
			// out of leader rebalancing until it has been stable for 5
			// minutes: moving leaders straight back onto it is what turns one
			// flap into a rebalancing loop. The re-election of leaders away
			// from a dead node is not affected.
			status := nc.GetStatus()
			return status.IsAvailable() && status.GetState() == proto.DataServerState_DATA_SERVER_STATE_RUNNING &&
				time.Since(status.GetLastTransitionTime()) >= 5*time.Minute
		},
	})

	c.rpc = rpcProvider(c.insID)

	// The data server controllers start running as soon as they are created,
	// and call back into the runtime under its lock: hold it until every
	// controller below is in place, so they never see the maps half-built.
	c.Lock()

	// init node controller
	for _, node := range dataServers {
		stored := dataServerStatus[node.GetNameOrDefault()].UnsafeBorrow().GetState()
		dataServer := &proto.DataServer{Identity: node, Metadata: &proto.DataServerMetadata{}}
		// A draining data server keeps the addresses that it drains from,
		// also when the configuration has its name again
		if configured, found := config.UnsafeBorrow().GetDataServer(node.GetNameOrDefault()); found &&
			stored != proto.DataServerState_DATA_SERVER_STATE_DRAINING {
			dataServer = configured
		}
		nc := dataservercontroller.NewController(
			c.ctx,
			dataServer,
			c.metadata,
			c,
			c,
			c.rpc,
			c.insID,
			oxiatime.SlowInitialInterval,
			health.DefaultCheckPolicy,
		)
		// Resume the drain before the lock is released, so that a draining
		// data server is never seen RUNNING
		if stored == proto.DataServerState_DATA_SERVER_STATE_DRAINING {
			nc.Drain()
		}
		c.dataServerControllers[node.GetNameOrDefault()] = nc
	}

	// init shard controller
	for ns, borrowedShards := range clusterStatus {
		shards := borrowedShards.UnsafeBorrow()
		for shard := range shards.Shards {
			shardMetadata := shards.Shards[shard]
			nsConfig, exist := namespaces[ns]
			if !exist {
				nsConfig = &proto.Namespace{}
			}
			//nolint:contextcheck // the shard controllers own their context
			c.shardControllers[shard] = shardcontroller.NewController(ns, shard, nsConfig,
				shardMetadata, c.metadata, c.findDataServerFeatures,
				c, c.rpc, shardcontroller.DefaultPeriodicTasksInterval)
		}
	}

	// Restart any in-progress splits from persisted state
	c.restartInProgressSplits(clusterStatus)
	c.Unlock()

	c.wg.Go(func() {
		process.DoWithLabels(c.ctx, map[string]string{
			"component": "coordinator-action-worker",
		}, c.startBackgroundActionWorker)
	})
	c.loadBalancer.Start()

	autoSplitInterval := config.UnsafeBorrow().GetAutoSplitWithDefaults().GetCollectionIntervalDurationOrDefault()
	c.autoSplitMonitor = autosplit.NewMonitor(c.metadata, c.rpc, c, autoSplitInterval)
	c.autoSplitMonitor.Start()

	return c, nil
}
