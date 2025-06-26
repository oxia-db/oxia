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

package balancer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/emirpasic/gods/v2/lists/arraylist"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/coordinator/actions"
	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/selectors"
	"github.com/oxia-db/oxia/coordinator/selectors/single"
	"github.com/oxia-db/oxia/coordinator/utils"
)

const defaultThreshElectedThreshold = 0.75

var _ LoadBalancer = &nodeBasedBalancer{}

type nodeBasedBalancer struct {
	*slog.Logger
	*sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	loadBalancerConf    *model.LoadBalancer
	statusResource      resources.StatusResource
	configResource      resources.ClusterConfigResource
	nodeAvailableJudger func(nodeID string) bool

	selector           selectors.Selector[*single.Context, string]
	loadRatioAlgorithm selectors.LoadRatioAlgorithm

	nodeQuarantineNodeMap   *sync.Map
	shardQuarantineShardMap *sync.Map

	triggerCh chan any

	actionCh chan actions.Action
}

func (r *nodeBasedBalancer) Action() <-chan actions.Action {
	return r.actionCh
}

func (r *nodeBasedBalancer) Close() error {
	close(r.triggerCh)
	r.cancel()
	r.Wait()
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
	currentStatus := r.statusResource.Load()
	candidates, metadata := r.configResource.NodesWithMetadata()
	groupedStatus, historyNodes := utils.GroupingShardsNodeByStatus(candidates, currentStatus)
	loadRatios := r.loadRatioAlgorithm(&model.RatioParams{NodeShardsInfos: groupedStatus, HistoryNodes: historyNodes})

	r.Info("start shard rebalance",
		slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
		slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
		slog.Any("quarantine-nodes", r.quarantineNodes()),
		slog.Float64("avg-shard-ratio", loadRatios.AvgShardLoadRatio()),
	)

	defer func() {
		swapGroup.Wait()
		r.Info("end shard rebalance",
			slog.Float64("max-node-load-ratio", loadRatios.MaxNodeLoadRatio()),
			slog.Float64("min-node-load-ratio", loadRatios.MinNodeLoadRatio()),
			slog.Float64("avg-shard-ratio", loadRatios.AvgShardLoadRatio()),
		)
	}()

	r.cleanDeletedNode(loadRatios, candidates, metadata, currentStatus, swapGroup)

	if loadRatios.RatioGap() <= loadRatios.AvgShardLoadRatio() {
		return true
	}
	r.balanceHighestNode(loadRatios, candidates, metadata, currentStatus, swapGroup)
	return false
}

func (r *nodeBasedBalancer) balanceHighestNode(loadRatios *model.Ratio, candidates *linkedhashset.Set[string], metadata map[string]model.ServerMetadata, currentStatus *model.ClusterStatus, swapGroup *sync.WaitGroup) {
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
		if _, err := r.swapShard(highestLoadRatioShard, fromNode, swapGroup, loadRatios, candidates, metadata, currentStatus); err != nil {
			r.Error("failed to select server when swap the node",
				slog.String("namespace", highestLoadRatioShard.Namespace),
				slog.Int64("shard", highestLoadRatioShard.ShardID),
				slog.String("from-node", fromNodeID),
				slog.Any("error", err),
			)
			continue
		}
		if !shardIter.Prev() {
			break
		}
	}
	if highestLoadRatioNode.Ratio-loadRatios.MinNodeLoadRatio() > loadRatios.AvgShardLoadRatio() {
		r.nodeQuarantineNodeMap.Store(highestLoadRatioNode.NodeID, time.Now())
		r.Info("can't rebalance the current node, quarantine it.",
			slog.Float64("highest-load-node-ratio", highestLoadRatioNode.Ratio),
			slog.String("highest-load-node", highestLoadRatioNode.NodeID))
	}
}

func (r *nodeBasedBalancer) cleanDeletedNode(loadRatios *model.Ratio,
	candidates *linkedhashset.Set[string],
	metadata map[string]model.ServerMetadata,
	currentStatus *model.ClusterStatus,
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
			if swapped, err := r.swapShard(shardRatio, deletedNode, swapGroup, loadRatios, candidates, metadata, currentStatus); err != nil || !swapped {
				r.Error("failed to select server when move ensemble out of deleted node",
					slog.String("namespace", shardRatio.Namespace),
					slog.Int64("shard", shardRatio.ShardID),
					slog.String("from-node", deletedNodeID),
					slog.Any("error", err),
				)
				continue
			}
		}
		if err := loadRatios.RemoveDeletedNode(nodeLoadRatio.NodeID); err != nil {
			r.Error("failed to remove deleted node from ratio snapshot", slog.Any("error", err))
		}
	}
}

func (r *nodeBasedBalancer) swapShard(
	candidateShard *model.ShardLoadRatio,
	fromNode model.Server,
	swapGroup *sync.WaitGroup,
	loadRatios *model.Ratio,
	candidates *linkedhashset.Set[string],
	metadata map[string]model.ServerMetadata,
	currentStatus *model.ClusterStatus) (bool, error) {
	var nsc *model.NamespaceConfig
	var exist bool
	var err error

	if nsc, exist = r.configResource.NamespaceConfig(candidateShard.Namespace); !exist {
		return false, nil
	}
	policies := nsc.Policies
	sContext := &single.Context{
		Candidates:         candidates,
		CandidatesMetadata: metadata,
		Policies:           policies,
		Status:             currentStatus,
		LoadRatioSupplier:  func() *model.Ratio { return loadRatios },
	}
	fromNodeID := fromNode.GetIdentifier()

	// filter selected
	selected := linkedhashset.New[string]()
	for _, candidate := range candidateShard.Ensemble {
		candidateID := candidate.GetIdentifier()
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
	var targetNode *model.Server
	if targetNode, exist = r.configResource.Node(targetNodeID); !exist {
		return false, errors.New("target node does not exist")
	}

	swapGroup.Add(1)
	r.actionCh <- &actions.SwapNodeAction{
		Shard:  candidateShard.ShardID,
		From:   fromNode,
		To:     *targetNode,
		Waiter: swapGroup,
	}
	r.Info("propose to swap the shard", slog.Int64("shard", candidateShard.ShardID), slog.Any("from", fromNode), slog.Any("to", targetNodeID))
	loadRatios.MoveShardToNode(candidateShard, fromNodeID, targetNodeID)
	loadRatios.ReCalculateRatios()
	return true, nil
}

func (r *nodeBasedBalancer) checkQuarantineNodes() {
	deletedKeys := make([]string, 0)
	r.nodeQuarantineNodeMap.Range(func(key, value any) bool {
		timestamp := value.(time.Time) //nolint: revive
		if time.Since(timestamp) >= r.loadBalancerConf.QuarantineTime {
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
		if time.Since(timestamp) >= r.loadBalancerConf.QuarantineTime {
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
	status := r.statusResource.Load()
	candidates := r.configResource.Nodes()
	groupedStatus, historyNodes := utils.GroupingShardsNodeByStatus(candidates, status)
	return r.loadRatioAlgorithm(
		&model.RatioParams{
			NodeShardsInfos: groupedStatus,
			HistoryNodes:    historyNodes,
			QuarantineNodes: r.quarantineNodes(),
		},
	).IsBalanced()
}

func (r *nodeBasedBalancer) Trigger() {
	r.Info("manually trigger balance")
	channel.PushNoBlock(r.triggerCh, nil)
}

func (r *nodeBasedBalancer) LoadRatioAlgorithm() selectors.LoadRatioAlgorithm {
	return r.loadRatioAlgorithm
}

func (r *nodeBasedBalancer) startBackgroundScheduler() {
	r.Add(1)
	go process.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-scheduler",
	}, func() {
		timer := time.NewTicker(r.loadBalancerConf.ScheduleInterval)
		defer timer.Stop()
		defer r.Done()
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
}

func (r *nodeBasedBalancer) startBackgroundNotifier() {
	r.Add(1)
	go process.DoWithLabels(r.ctx, map[string]string{
		"component": "load-balancer-notifier",
	}, func() {
		defer r.Done()
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
}

func (r *nodeBasedBalancer) rebalanceLeader() {
	r.checkQuarantineShards()

	status := r.statusResource.Load()
	candidates := r.configResource.Nodes()
	totalShards, electedShards, nodeLeaders := utils.NodeShardLeaders(candidates, status)

	electedRate := float64(electedShards) / float64(totalShards)
	if electedRate < defaultThreshElectedThreshold {
		// leader does not inited
		return
	}

	r.Info("start leader rebalance",
		slog.Any("node-leaders", nodeLeaders),
		slog.Any("quarantine-shards", r.quarantineShards()),
	)

	defer func() {
		r.Info("end leader rebalance")
	}()

	maxLeadersNodeID, maxLeaders, minLeaders := r.rankLeaders(nodeLeaders)

	nsAndShards := nodeLeaders[maxLeadersNodeID]

	for iter := nsAndShards.Iterator(); iter.Next(); {
		nsAndShard := iter.Value()
		if maxLeaders-minLeaders <= 1 {
			break
		}
		if _, exist := r.shardQuarantineShardMap.Load(nsAndShard.ShardID); exist {
			continue
		}
		namespace := nsAndShard.Namespace
		shard := nsAndShard.ShardID
		shardStatus := status.Namespaces[namespace].Shards[shard]

		minCandidateLeaders := -1
		for _, candidate := range shardStatus.Ensemble {
			candidateID := candidate.GetIdentifier()
			if !r.nodeAvailableJudger(candidateID) {
				continue
			}
			shards := nodeLeaders[candidateID]
			leaders := shards.Size()
			if minCandidateLeaders == -1 || leaders < minCandidateLeaders {
				minCandidateLeaders = leaders
			}
		}
		if minCandidateLeaders == maxLeaders || minCandidateLeaders+1 == maxLeaders {
			r.Info("quarantine the shard due to no valid candidates", slog.Int64("shard", shard), slog.Any("leader", maxLeadersNodeID))
			r.shardQuarantineShardMap.Store(shard, time.Now())
			break
		}

		latch := &sync.WaitGroup{}
		latch.Add(1)
		ac := &actions.ElectionAction{
			Shard:  shard,
			Waiter: latch,
		}
		r.actionCh <- ac
		latch.Wait()
		leader := ac.NewLeader
		r.Info("triggered new election", slog.Int64("shard", shard), slog.Any("old-leader", maxLeadersNodeID), slog.Any("new-leader", leader))
		if leader == maxLeadersNodeID { // no changes
			r.Info("quarantine the shard due to no leader changed", slog.Int64("shard", shard), slog.Any("old-leader", maxLeadersNodeID), slog.Any("new-leader", leader))
			r.shardQuarantineShardMap.Store(shard, time.Now())
		}
		break
	}
}

func (r *nodeBasedBalancer) rankLeaders(nodeLeaders map[string]*arraylist.List[utils.NamespaceAndShard]) (maxLeadersNodeID string, maxLeaders int, minLeaders int) {
	maxLeadersNodeID = ""
	maxLeaders = -1
	minLeaders = -1
	for nodeID, nsAndShards := range nodeLeaders {
		if nsAndShards.Size() > 0 {
			existNonQuarantineShard := nsAndShards.Any(func(_ int, value utils.NamespaceAndShard) bool {
				_, exist := r.shardQuarantineShardMap.Load(value.ShardID)
				return !exist
			})
			if !existNonQuarantineShard { // Filter out quarantined nodes
				continue
			}
		}
		leaderNum := nsAndShards.Size()
		if maxLeaders == -1 {
			maxLeaders = leaderNum
			minLeaders = leaderNum
			maxLeadersNodeID = nodeID
			continue
		}
		if leaderNum > maxLeaders {
			maxLeaders = leaderNum
			maxLeadersNodeID = nodeID
			continue
		}
		if leaderNum < minLeaders {
			minLeaders = leaderNum
		}
	}
	return maxLeadersNodeID, maxLeaders, minLeaders
}

func NewLoadBalancer(options Options) LoadBalancer {
	ctx, cancelFunc := context.WithCancel(options.Context)
	logger := slog.With(
		slog.String("component", "load-balancer"))

	nb := &nodeBasedBalancer{
		WaitGroup:               &sync.WaitGroup{},
		Logger:                  logger,
		ctx:                     ctx,
		cancel:                  cancelFunc,
		loadBalancerConf:        options.ClusterConfigResource.LoadLoadBalancer(),
		actionCh:                make(chan actions.Action, 1000),
		nodeAvailableJudger:     options.NodeAvailableJudger,
		statusResource:          options.StatusResource,
		configResource:          options.ClusterConfigResource,
		selector:                single.NewSelector(),
		loadRatioAlgorithm:      single.DefaultShardsRank,
		nodeQuarantineNodeMap:   &sync.Map{},
		shardQuarantineShardMap: &sync.Map{},
		triggerCh:               make(chan any, 1),
	}
	nb.startBackgroundScheduler()
	nb.startBackgroundNotifier()
	return nb
}
