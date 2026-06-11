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

package balancer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/single"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/state"

	"github.com/oxia-db/oxia/common/concurrent"

	"github.com/oxia-db/oxia/common/process"

	"github.com/oxia-db/oxia/common/channel"
)

const defaultThreshElectedThreshold = 0.75

var _ LoadBalancer = &nodeBasedBalancer{}

type nodeBasedBalancer struct {
	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup

	loadBalancerConf    *commonproto.LoadBalancer
	metadata            coordmetadata.Metadata
	nodeAvailableJudger func(nodeID string) bool

	selector           selector.Selector[*single.Context, string]
	loadRatioAlgorithm selector.LoadRatioAlgorithm

	nodeQuarantineNodeMap   *sync.Map
	shardQuarantineShardMap *sync.Map

	triggerCh chan any

	actionCh chan action.Action
}

func (r *nodeBasedBalancer) Action() <-chan action.Action {
	return r.actionCh
}

func (r *nodeBasedBalancer) Close() error {
	close(r.triggerCh)
	r.ctxCancel()
	r.wg.Wait()
	return nil
}

func (r *nodeBasedBalancer) quarantineNodes() *linkedhashset.Set[string] {
	nodes := linkedhashset.New[string]()
	r.nodeQuarantineNodeMap.Range(func(nodeID, _ any) bool {
		nodes.Add(nodeID.(string))
		return true
	})
	return nodes
}

func dataServersToCandidatesAndMetadata(dataServers map[string]commonobject.Borrowed[*commonproto.DataServer]) (
	*linkedhashset.Set[string],
	map[string]*commonproto.DataServerMetadata,
) {
	candidates := linkedhashset.New[string]()
	metadata := make(map[string]*commonproto.DataServerMetadata, len(dataServers))
	for name, borrowedDataServer := range dataServers {
		dataServer := borrowedDataServer.UnsafeBorrow()
		candidates.Add(name)
		if dataServer.GetMetadata() != nil {
			metadata[name] = dataServer.GetMetadata()
			continue
		}
		metadata[name] = &commonproto.DataServerMetadata{}
	}
	return candidates, metadata
}

func (r *nodeBasedBalancer) quarantineShards() *linkedhashset.Set[int64] {
	shards := linkedhashset.New[int64]()
	r.shardQuarantineShardMap.Range(func(shardID, _ any) bool {
		shards.Add(shardID.(int64))
		return true
	})
	return shards
}

func (r *nodeBasedBalancer) rebalanceEnsemble() bool {
	r.checkQuarantineNodes()

	swapGroup := &sync.WaitGroup{}
	currentStatus := r.metadata.ListNamespaceStatus()
	dataServers := r.metadata.ListDataServer()
	candidates, metadata := dataServersToCandidatesAndMetadata(dataServers)
	groupedStatus, historyNodes := state.GroupingShardsNodeByStatus(candidates, currentStatus)
	loadRatios := r.loadRatioAlgorithm(&model.RatioParams{NodeShardsInfos: groupedStatus, HistoryNodes: historyNodes})

	if r.logger.Enabled(r.ctx, slog.LevelDebug) {
		r.logger.Debug("start shard rebalance",
			slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
			slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
			slog.Any("quarantine-nodes", r.quarantineNodes()),
			slog.Float64("avg-shard-ratio", loadRatios.AvgShardLoadRatio()),
		)
	}

	defer func() {
		swapGroup.Wait()
		if r.logger.Enabled(r.ctx, slog.LevelDebug) {
			r.logger.Debug("end shard rebalance",
				slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
				slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
				slog.Float64("avg-shard-ratio", loadRatios.AvgShardLoadRatio()),
			)
		}
	}()

	r.cleanDeletedNode(loadRatios, candidates, metadata, swapGroup)

	if loadRatios.RatioGap() <= loadRatios.AvgShardLoadRatio() {
		return true
	}
	r.balanceHighestNode(loadRatios, candidates, metadata, swapGroup)
	return false
}

func (r *nodeBasedBalancer) balanceHighestNode(loadRatios *model.Ratio, candidates *linkedhashset.Set[string], metadata map[string]*commonproto.DataServerMetadata, swapGroup *sync.WaitGroup) {
	nodeIter := loadRatios.NodeIterator()
	if !nodeIter.Last() {
		return
	}
	var highestLoadRatioNode *model.NodeLoadRatio
	for {
		if highestLoadRatioNode = nodeIter.Value(); highestLoadRatioNode == nil {
			return // unexpected
		}
		if !r.IsNodeQuarantined(highestLoadRatioNode) {
			break
		}
		if nodeIter.Prev() {
			continue
		}
		break
	}
	shardIter := highestLoadRatioNode.ShardIterator()
	if !shardIter.Last() {
		return
	}
	for highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		var highestLoadRatioShard *model.ShardLoadRatio
		if highestLoadRatioShard = shardIter.Value(); highestLoadRatioShard == nil {
			break
		}
		fromNodeID := highestLoadRatioNode.NodeID
		fromNode := highestLoadRatioNode.Node
		if _, err := r.swapShard(highestLoadRatioShard, fromNode, swapGroup, loadRatios, candidates, metadata); err != nil {
			r.logger.Error("failed to select server when swap the node",
				slog.String("namespace", highestLoadRatioShard.Namespace),
				slog.Int64("shard", highestLoadRatioShard.ShardID),
				slog.String("from-node", fromNodeID),
				slog.Any("error", err),
			)
		}
		if !shardIter.Prev() {
			break
		}
	}
	if highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		r.nodeQuarantineNodeMap.Store(highestLoadRatioNode.NodeID, time.Now())
		r.logger.Info("can't rebalance the current node, quarantine it.",
			slog.Float64("highest-load-node-ratio", highestLoadRatioNode.Ratio),
			slog.String("highest-load-node", highestLoadRatioNode.NodeID))
	}
}

func (r *nodeBasedBalancer) cleanDeletedNode(loadRatios *model.Ratio,
	candidates *linkedhashset.Set[string],
	metadata map[string]*commonproto.DataServerMetadata,
	swapGroup *sync.WaitGroup) {
	for nodeIter := loadRatios.NodeIterator(); nodeIter.Next(); {
		nodeLoadRatio := nodeIter.Value()
		if candidates.Contains(nodeLoadRatio.NodeID) {
			continue
		}
		deletedNodeID := nodeLoadRatio.NodeID
		deletedNode := nodeLoadRatio.Node
		for shardIter := nodeLoadRatio.ShardIterator(); shardIter.Next(); {
			var shardRatio = shardIter.Value()
			if swapped, err := r.swapShard(shardRatio, deletedNode, swapGroup, loadRatios, candidates, metadata); err != nil || !swapped {
				r.logger.Error("failed to select server when move ensemble out of deleted node",
					slog.String("namespace", shardRatio.Namespace),
					slog.Int64("shard", shardRatio.ShardID),
					slog.String("from-node", deletedNodeID),
					slog.Any("error", err),
				)
				continue
			}
		}
		if err := loadRatios.RemoveDeletedNode(nodeLoadRatio.NodeID); err != nil {
			r.logger.Error("failed to remove deleted node from ratio snapshot", slog.Any("error", err))
		}
	}
}

func (r *nodeBasedBalancer) swapShard(
	candidateShard *model.ShardLoadRatio,
	fromNode *commonproto.DataServerIdentity,
	swapGroup *sync.WaitGroup,
	loadRatios *model.Ratio,
	candidates *linkedhashset.Set[string],
	metadata map[string]*commonproto.DataServerMetadata) (bool, error) {
	var nsc *commonproto.Namespace
	var exist bool
	var err error

	var borrowedNamespace commonobject.Borrowed[*commonproto.Namespace]
	if borrowedNamespace, exist = r.metadata.GetNamespace(candidateShard.Namespace); !exist {
		return false, nil
	}
	nsc = borrowedNamespace.UnsafeBorrow()

	// With RF=1, an ensemble swap cannot safely transfer data (there's no
	// follower to replicate from). Skip rebalancing for such namespaces.
	if nsc.GetReplicationFactor() <= 1 {
		return false, nil
	}

	sContext := &single.Context{
		Candidates:         candidates,
		CandidatesMetadata: metadata,
		AntiAffinities:     nsc.GetAntiAffinities(),
		Namespace:          candidateShard.Namespace,
		Shard:              candidateShard.ShardID,
		LoadRatioSupplier:  func() *model.Ratio { return loadRatios },
	}
	fromNodeID := fromNode.GetNameOrDefault()

	// filter selected
	selected := linkedhashset.New[string]()
	for _, candidate := range candidateShard.Ensemble {
		candidateID := candidate.GetNameOrDefault()
		if candidateID == fromNodeID {
			continue
		}
		selected.Add(candidateID)
	}
	sContext.SetSelected(selected)

	var targetNodeID string
	if targetNodeID, err = r.selector.Select(sContext); err != nil {
		return false, err
	}
	if targetNodeID == fromNodeID {
		return false, nil
	}
	var targetNode *commonproto.DataServerIdentity
	var borrowedTargetNode commonobject.Borrowed[*commonproto.DataServer]
	if borrowedTargetNode, exist = r.metadata.GetDataServer(targetNodeID); !exist {
		return false, errors.New("target node does not exist")
	}
	targetNode = borrowedTargetNode.UnsafeBorrow().GetIdentity()
	if targetNode == nil {
		return false, errors.New("target node does not have identity")
	}

	r.logger.Info("propose to swap the shard", slog.Int64("shard", candidateShard.ShardID), slog.Any("from", fromNode), slog.Any("to", targetNodeID))
	swapGroup.Add(1)
	r.actionCh <- action.NewChangeEnsembleActionWithCallback(candidateShard.ShardID, fromNode, targetNode,
		concurrent.NewOnce(func(_ any) {
			swapGroup.Done()
		}, func(_ error) {
			swapGroup.Done()
		}))
	loadRatios.MoveShardToNode(candidateShard, fromNodeID, targetNodeID)
	loadRatios.ReCalculateRatios()
	return true, nil
}

func (r *nodeBasedBalancer) checkQuarantineNodes() {
	deletedKeys := make([]string, 0)
	r.nodeQuarantineNodeMap.Range(func(key, value any) bool {
		timestamp := value.(time.Time) //nolint: revive
		if time.Since(timestamp) >= r.loadBalancerConf.GetQuarantineTimeDurationOrDefault() {
			deletedKeys = append(deletedKeys, key.(string))
		}
		return true
	})
	for _, key := range deletedKeys {
		r.nodeQuarantineNodeMap.Delete(key)
	}
}

func (r *nodeBasedBalancer) checkQuarantineShards() {
	deletedKeys := make([]int64, 0)
	r.shardQuarantineShardMap.Range(func(key, value any) bool {
		timestamp := value.(time.Time) //nolint: revive
		if time.Since(timestamp) >= r.loadBalancerConf.GetQuarantineTimeDurationOrDefault() {
			deletedKeys = append(deletedKeys, key.(int64))
		}
		return true
	})
	for _, key := range deletedKeys {
		r.shardQuarantineShardMap.Delete(key)
	}
}

func (r *nodeBasedBalancer) IsNodeQuarantined(highestLoadRatioNode *model.NodeLoadRatio) bool {
	_, found := r.nodeQuarantineNodeMap.Load(highestLoadRatioNode.NodeID)
	return found
}

func (r *nodeBasedBalancer) IsBalanced() bool {
	configNamespaces := r.metadata.ListNamespace()
	if len(configNamespaces) != len(r.metadata.ListNamespaceStatus()) {
		return false
	}
	for namespace := range configNamespaces {
		if _, exists := r.metadata.GetNamespaceStatus(namespace); !exists {
			return false
		}
	}

	status := r.metadata.ListNamespaceStatus()
	candidates, _ := dataServersToCandidatesAndMetadata(r.metadata.ListDataServer())
	groupedStatus, historyNodes := state.GroupingShardsNodeByStatus(candidates, status)
	shardsBalanced := r.loadRatioAlgorithm(
		&model.RatioParams{
			NodeShardsInfos: groupedStatus,
			HistoryNodes:    historyNodes,
			QuarantineNodes: r.quarantineNodes(),
		},
	).IsBalanced()
	if !shardsBalanced {
		return false
	}
	return r.leaderBalanced(candidates, status)
}

func (r *nodeBasedBalancer) leaderBalanced(candidates *linkedhashset.Set[string], status map[string]commonobject.Borrowed[*commonproto.NamespaceStatus]) bool {
	totalShards, electedShards, _ := state.NodeShardLeaders(candidates, status)
	if totalShards == 0 {
		return true
	}
	if electedShards != totalShards {
		return false
	}

	// The leaders are balanced when the rebalancer has no move left to make.
	// This must use the same move search as rebalanceLeader: a stricter
	// criterion (e.g. a global max-min spread) can be unreachable when the
	// less-loaded nodes do not belong to the ensembles of the shards led by
	// the most-loaded ones, and the balancer would never report the cluster
	// as balanced while having no valid move left.
	_, found := r.bestLeaderMove(candidates, status, false)
	return !found
}

// bestLeaderMove looks for the shard whose leadership, if moved to the least
// loaded node of its ensemble, yields the biggest improvement of the leader
// balance. A move is only an improvement when the gap between the current
// leader's count and the target's count is at least 2 (each move then strictly
// reduces the overall imbalance, so repeated moves always terminate). Only
// available nodes are considered, both as donors and as targets. When
// skipQuarantined is set, shards currently quarantined are not eligible.
func (r *nodeBasedBalancer) bestLeaderMove(candidates *linkedhashset.Set[string],
	status map[string]commonobject.Borrowed[*commonproto.NamespaceStatus],
	skipQuarantined bool) (move state.NamespaceAndShard, found bool) {
	_, _, nodeLeaders := state.NodeShardLeaders(candidates, status)

	counts := make(map[string]int, len(nodeLeaders))
	for nodeID, nsAndShards := range nodeLeaders {
		if r.nodeAvailableJudger(nodeID) {
			counts[nodeID] = nsAndShards.Size()
		}
	}

	bestGap := 1 // a move is an improvement only when the gap is >= 2
	for nodeID, nsAndShards := range nodeLeaders {
		donorLeaders, available := counts[nodeID]
		if !available {
			// Leaders on a non-running node are re-elected by the data server
			// failure handling, not by the balancer
			continue
		}
		for iter := nsAndShards.Iterator(); iter.Next(); {
			nsAndShard := iter.Value()
			if skipQuarantined {
				if _, exist := r.shardQuarantineShardMap.Load(nsAndShard.ShardID); exist {
					continue
				}
			}
			shardStatus := status[nsAndShard.Namespace].UnsafeBorrow().Shards[nsAndShard.ShardID]
			for _, candidate := range shardStatus.Ensemble {
				candidateLeaders, available := counts[candidate.GetNameOrDefault()]
				if !available {
					continue
				}
				if donorLeaders-candidateLeaders > bestGap {
					bestGap = donorLeaders - candidateLeaders
					move = nsAndShard
					found = true
				}
			}
		}
	}
	return move, found
}

func (r *nodeBasedBalancer) Trigger() {
	r.logger.Info("manually trigger balance")
	channel.PushNoBlock(r.triggerCh, nil)
}

func (r *nodeBasedBalancer) LoadRatioAlgorithm() selector.LoadRatioAlgorithm {
	return r.loadRatioAlgorithm
}

func (r *nodeBasedBalancer) startBackgroundScheduler() {
	r.wg.Go(func() {
		process.DoWithLabels(r.ctx, map[string]string{
			"component": "load-balancer-scheduler",
		}, func() {
			timer := time.NewTicker(r.loadBalancerConf.GetScheduleIntervalDurationOrDefault())
			defer timer.Stop()
			for {
				select {
				case _, more := <-timer.C:
					if !more {
						return
					}
					channel.PushNoBlock(r.triggerCh, nil)
				case <-r.ctx.Done():
					return
				}
			}
		})
	})
}

func (r *nodeBasedBalancer) startBackgroundNotifier() {
	r.wg.Go(func() {
		process.DoWithLabels(r.ctx, map[string]string{
			"component": "load-balancer-notifier",
		}, func() {
			for {
				select {
				case _, more := <-r.triggerCh:
					if !more {
						return
					}
					if r.rebalanceEnsemble() { // if shard is balanced
						r.rebalanceLeader()
					}
				case <-r.ctx.Done():
					return
				}
			}
		})
	})
}

func (r *nodeBasedBalancer) rebalanceLeader() {
	r.checkQuarantineShards()

	status := r.metadata.ListNamespaceStatus()
	candidates, _ := dataServersToCandidatesAndMetadata(r.metadata.ListDataServer())
	totalShards, electedShards, nodeLeaders := state.NodeShardLeaders(candidates, status)

	electedRate := float64(electedShards) / float64(totalShards)
	if electedRate < defaultThreshElectedThreshold {
		// leader does not inited
		return
	}

	if r.logger.Enabled(r.ctx, slog.LevelDebug) {
		r.logger.Debug("start leader rebalance",
			slog.Any("node-leaders", nodeLeaders),
			slog.Any("quarantine-shards", r.quarantineShards()),
		)
		defer func() {
			r.logger.Debug("end leader rebalance")
		}()
	}

	move, found := r.bestLeaderMove(candidates, status, true)
	if !found {
		return
	}
	shard := move.ShardID
	oldLeader := ""
	if shardStatus := status[move.Namespace].UnsafeBorrow().Shards[shard]; shardStatus.Leader != nil {
		oldLeader = shardStatus.Leader.GetNameOrDefault()
	}

	latch := &sync.WaitGroup{}
	latch.Add(1)
	ac := &action.ElectionAction{
		Shard:  shard,
		Waiter: latch,
	}
	r.actionCh <- ac
	latch.Wait()
	leader := ac.NewLeader
	r.logger.Info("triggered new election", slog.Int64("shard", shard), slog.Any("old-leader", oldLeader), slog.Any("new-leader", leader))
	if leader == oldLeader { // no changes
		r.logger.Info("quarantine the shard due to no leader changed", slog.Int64("shard", shard), slog.Any("old-leader", oldLeader), slog.Any("new-leader", leader))
		r.shardQuarantineShardMap.Store(shard, time.Now())
	}
}

func (r *nodeBasedBalancer) Start() {
	r.startBackgroundScheduler()
	r.startBackgroundNotifier()
}

func NewLoadBalancer(options Options) LoadBalancer {
	ctx, cancelFunc := context.WithCancel(options.Context)
	logger := slog.With(
		slog.String("component", "load-balancer"))

	return &nodeBasedBalancer{
		logger:                  logger,
		ctx:                     ctx,
		ctxCancel:               cancelFunc,
		wg:                      sync.WaitGroup{},
		loadBalancerConf:        options.Metadata.GetLoadBalancer().UnsafeBorrow(),
		actionCh:                make(chan action.Action, 1000),
		nodeAvailableJudger:     options.NodeAvailableJudger,
		metadata:                options.Metadata,
		selector:                single.NewSelector(),
		loadRatioAlgorithm:      single.DefaultShardsRank,
		nodeQuarantineNodeMap:   &sync.Map{},
		shardQuarantineShardMap: &sync.Map{},
		triggerCh:               make(chan any, 1),
	}
}
