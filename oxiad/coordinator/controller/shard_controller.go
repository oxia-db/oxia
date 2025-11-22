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

package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/selector/leader"

	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
)

const (
	// When fencing quorum of servers, after we reach the majority, wait a bit more
	// to include responses from all healthy servers.
	quorumFencingGracePeriod = 100 * time.Millisecond

	chanBufferSize = 100

	DefaultPeriodicTasksInterval = 1 * time.Minute
)

var _ ShardController = &shardController{}

// The ShardController is responsible to handle all the state transition for a given a shard
// e.g. electing a new leader.
type ShardController interface {
	io.Closer
	NodeEventListener

	Metadata() *Metadata

	SyncServerAddress()

	DeleteShard()

	Election(action *action.ElectionAction) string

	ChangeEnsemble(action *action.ChangeEnsembleAction)
}

type shardController struct {
	namespace       string
	shard           int64
	namespaceConfig *model.NamespaceConfig
	rpc             rpc.Provider
	metadata        Metadata

	leaderSelector selector.Selector[*leaderselector.Context, model.Server]

	eventListener  ShardEventListener
	configResource resource.ClusterConfigResource
	statusResource resource.StatusResource

	electionOp       chan *action.ElectionAction
	deleteOp         chan any
	nodeFailureOp    chan model.Server
	changeEnsembleOp chan *action.ChangeEnsembleAction

	ctx                   context.Context
	cancel                context.CancelFunc
	wg                    *sync.WaitGroup
	periodicTasksInterval time.Duration
	log                   *slog.Logger

	currentElection *ShardElection

	leaderElectionLatency metric.LatencyHistogram
	newTermQuorumLatency  metric.LatencyHistogram
	becomeLeaderLatency   metric.LatencyHistogram
	leaderElectionsFailed metric.Counter
	termGauge             metric.Gauge
}

func (s *shardController) Metadata() *Metadata {
	return &s.metadata
}

func (s *shardController) NodeBecameUnavailable(node model.Server) {
	s.nodeFailureOp <- node
}

//nolint:revive
func NewShardController(
	namespace string,
	shard int64,
	nc *model.NamespaceConfig,
	shardMetadata model.ShardMetadata,
	configResource resource.ClusterConfigResource,
	statusResource resource.StatusResource,
	eventListener ShardEventListener,
	rpcProvider rpc.Provider,
	periodTasksInterval time.Duration) ShardController {
	labels := metric.LabelsForShard(namespace, shard)
	s := &shardController{
		namespace:        namespace,
		shard:            shard,
		namespaceConfig:  nc,
		metadata:         NewMetadata(shardMetadata),
		rpc:              rpcProvider,
		configResource:   configResource,
		statusResource:   statusResource,
		eventListener:    eventListener,
		leaderSelector:   leaderselector.NewSelector(),
		electionOp:       make(chan *action.ElectionAction, chanBufferSize),
		deleteOp:         make(chan any, chanBufferSize),
		nodeFailureOp:    make(chan model.Server, chanBufferSize),
		changeEnsembleOp: make(chan *action.ChangeEnsembleAction, chanBufferSize),

		periodicTasksInterval: periodTasksInterval,
		log: slog.With(
			slog.String("component", "shard-controller"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shard),
		),
		wg: &sync.WaitGroup{},

		leaderElectionLatency: metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		leaderElectionsFailed: metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels),
		newTermQuorumLatency: metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of nodes to a new term", labels),
		becomeLeaderLatency: metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
	}

	s.termGauge = metric.NewGauge("oxia_coordinator_term",
		"The term of the shard", "count", labels, func() int64 {
			return s.metadata.Term()
		})

	s.ctx, s.cancel = context.WithCancel(context.Background())

	shardMeta := s.metadata.Load()
	s.log.Info("Started shard controller", slog.Any("shard-metadata", shardMeta))

	s.wg.Go(func() {
		process.DoWithLabels(
			s.ctx,
			map[string]string{
				"oxia":      "shard-controller",
				"namespace": s.namespace,
				"shard":     fmt.Sprintf("%d", s.shard),
			}, func() {
				s.run(&shardMeta)
			},
		)
	})

	return s
}

func (s *shardController) Election(action *action.ElectionAction) string {
	clonedAction := action.Clone()
	clonedAction.Waiter.Add(1)
	s.electionOp <- clonedAction
	clonedAction.Waiter.Wait()
	return clonedAction.NewLeader
}

func (s *shardController) run(initShardMeta *model.ShardMetadata) {
	// Do initial check or leader election
	switch {
	case initShardMeta.Status == model.ShardStatusDeleting:
		s.DeleteShard()
	case initShardMeta.Leader == nil || initShardMeta.Status != model.ShardStatusSteadyState:
		s.onElectLeader(nil)
	default:
		s.log.Info(
			"There is already a node marked as leader on the shard, verifying",
			slog.Any("current-leader", initShardMeta.Leader),
		)

		if !s.verifyCurrentEnsemble(initShardMeta) {
			s.onElectLeader(nil)
		} else {
			s.SyncServerAddress()
		}
	}

	s.log.Info("Shard is ready", slog.Any("leader", initShardMeta.Leader))

	periodicTasksTimer := time.NewTicker(s.periodicTasksInterval)

	for {
		select {
		case <-s.ctx.Done():
			return

		case <-s.deleteOp:
			s.deleteShardWithRetries()
		case n := <-s.nodeFailureOp:
			s.handleNodeFailure(n)
		case op := <-s.changeEnsembleOp:
			s.onChangeEnsemble(op)
		case <-periodicTasksTimer.C:
			s.handlePeriodicTasks()
		case electionAction := <-s.electionOp:
			newLeader := s.onElectLeader(nil)
			electionAction.Done(newLeader.GetIdentifier())
		}
	}
}

func (s *shardController) handleNodeFailure(failedNode model.Server) {
	shardMeta := s.metadata.Load()
	s.log.Debug(
		"Received notification of failed node",
		slog.Any("failed-node", failedNode),
		slog.Any("current-leader", shardMeta.Leader),
	)

	if shardMeta.Leader != nil &&
		shardMeta.Leader.GetIdentifier() == failedNode.GetIdentifier() {
		s.log.Info(
			"Detected failure on shard leader",
			slog.Any("leader", failedNode),
		)
		s.onElectLeader(nil)
	}
}

func (s *shardController) verifyCurrentEnsemble(initShardMeta *model.ShardMetadata) bool {
	// Ideally, we shouldn't need to trigger a new leader election if a follower
	// is out of sync. We should just go back into the retry-to-fence follower
	// loop. In practice, the current approach is easier for now.
	for _, node := range initShardMeta.Ensemble {
		nodeStatus, err := s.rpc.GetStatus(s.ctx, node, &proto.GetStatusRequest{Shard: s.shard})

		switch {
		case err != nil:
			s.log.Warn(
				"Failed to verify status for shard. Start a new election",
				slog.Any("error", err),
				slog.Any("node", node),
			)
			return false
		case node.GetIdentifier() == initShardMeta.Leader.GetIdentifier() &&
			nodeStatus.Status != proto.ServingStatus_LEADER:
			s.log.Warn(
				"Expected leader is not in leader status. Start a new election",
				slog.Any("node", node),
				slog.Any("status", nodeStatus.Status),
			)
			return false
		case node.GetIdentifier() != initShardMeta.Leader.GetIdentifier() &&
			nodeStatus.Status != proto.ServingStatus_FOLLOWER:
			s.log.Warn(
				"Expected follower is not in follower status. Start a new election",
				slog.Any("node", node),
				slog.Any("status", nodeStatus.Status),
			)
			return false
		case nodeStatus.Term != initShardMeta.Term:
			s.log.Warn(
				"Node has a wrong term. Start a new election",
				slog.Any("node", node),
				slog.Any("node-term", nodeStatus.Term),
				slog.Any("coordinator-term", initShardMeta.Term),
			)
			return false
		default:
			s.log.Info(
				"Node looks ok",
				slog.Any("node", node),
			)
		}
	}

	s.log.Info("All nodes look good. No need to trigger new leader election")
	return true
}

func (s *shardController) onElectLeader(action *action.ChangeEnsembleAction) model.Server {
	// stop the current term election
	if s.currentElection != nil {
		s.currentElection.Stop()
		s.currentElection = nil
	}
	termOptions := &proto.NewTermOptions{
		EnableNotifications: true,
		KeySorting:          proto.KeySortingType_UNKNOWN,
	}
	nsConfig, exist := s.configResource.NamespaceConfig(s.namespace)
	if exist {
		termOptions.EnableNotifications = nsConfig.NotificationsEnabled.Get()
		termOptions.KeySorting = nsConfig.KeySorting.ToProto()
	}
	s.currentElection = NewShardElection(s.ctx, s.log, s.eventListener,
		s.statusResource, s.configResource, s.leaderSelector,
		s.rpc, &s.metadata, s.namespace, s.shard, action,
		termOptions,
		s.leaderElectionLatency,
		s.newTermQuorumLatency,
		s.becomeLeaderLatency,
		s.leaderElectionsFailed)
	leaderNode := s.currentElection.Start()
	return leaderNode
}

func (s *shardController) deleteShardRpc(ctx context.Context, node model.Server) error {
	_, err := s.rpc.DeleteShard(ctx, node, &proto.DeleteShardRequest{
		Namespace: s.namespace,
		Shard:     s.shard,
		Term:      s.metadata.Term(),
	})

	return err
}

func (s *shardController) DeleteShard() {
	s.deleteOp <- nil
}

func (s *shardController) deleteShardWithRetries() {
	s.log.Info("Deleting shard")

	_ = backoff.RetryNotify(s.deleteShard, oxiatime.NewBackOff(s.ctx),
		func(err error, duration time.Duration) {
			s.log.Warn(
				"Delete shard failed, retrying later",
				slog.Duration("retry-after", duration),
				slog.Any("error", err),
			)
		})

	s.cancel()
}

func (s *shardController) deleteShard() error {
	shardMeta := s.metadata.Load()
	for _, server := range shardMeta.Ensemble {
		// We need to save the address because it gets modified in the loop
		if err := s.deleteShardRpc(s.ctx, server); err != nil {
			s.log.Warn(
				"Failed to delete shard",
				slog.Any("error", err),
				slog.Any("node", server),
			)
			return err
		}

		s.log.Info(
			"Successfully deleted shard from node",
			slog.Any("server-address", server),
		)
	}

	s.statusResource.DeleteShardMetadata(s.namespace, s.shard)
	s.eventListener.ShardDeleted(s.shard)
	return s.close()
}

func (s *shardController) Close() error {
	err := s.close()
	if err != nil {
		return err
	}

	// NOTE: we must wait the run goroutine to exit, otherwise
	// the controller maybe running after close is returned.
	s.wg.Wait()
	return nil
}

func (s *shardController) close() error {
	s.cancel()
	s.termGauge.Unregister()
	return nil
}

func (s *shardController) ChangeEnsemble(action *action.ChangeEnsembleAction) {
	s.changeEnsembleOp <- action
}

func (s *shardController) onChangeEnsemble(action *action.ChangeEnsembleAction) {
	var err error
	defer func() {
		if err != nil {
			action.Error(err)
		} else {
			action.Done(nil)
		}
	}()
	if s.currentElection != nil {
		if ready := s.currentElection.IsReadyForChangeEnsemble(); !ready {
			err = ErrNotReadyForChangeEnsemble
			return
		}
	}
	// todo: support optimized ensemble change to avoid start a new election
	s.onElectLeader(action)
}
func (s *shardController) SyncServerAddress() {
	shardMeta := s.metadata.Load()
	needSync := false
	for _, candidate := range shardMeta.Ensemble {
		if newInfo, ok := s.configResource.Node(candidate.GetIdentifier()); ok {
			if newInfo.Public != candidate.Public || newInfo.Internal != candidate.Internal {
				needSync = true
				break
			}
		}
	}
	if !needSync {
		return
	}
	s.log.Info("server address changed, start a new leader election")
	group := &sync.WaitGroup{}
	group.Add(1)
	s.electionOp <- &action.ElectionAction{
		Shard:  s.shard,
		Waiter: group,
	}
}

func (s *shardController) handlePeriodicTasks() {
	mutShardMeta := s.metadata.Load()

	if len(mutShardMeta.PendingDeleteShardNodes) > 0 {
		var err error
		if err = s.handlePendingDeleteShardNodes(&mutShardMeta); err != nil {
			s.log.Warn("Failed to handle pending delete shard nodes", "error", err)
			return
		}
	}

	// Update the shard status
	s.statusResource.UpdateShardMetadata(s.namespace, s.shard, mutShardMeta)
	s.metadata.Store(mutShardMeta)
}

func (s *shardController) handlePendingDeleteShardNodes(mutShardMeta *model.ShardMetadata) error {
	for _, ds := range mutShardMeta.PendingDeleteShardNodes {
		s.log.Info("Deleting shard from removed node", "node", ds)

		if _, err := s.rpc.DeleteShard(s.ctx, ds, &proto.DeleteShardRequest{
			Namespace: s.namespace,
			Shard:     s.shard,
			Term:      mutShardMeta.Term,
		}); err != nil {
			s.log.Warn("Failed to delete shard from removed node", "node", ds, "error", err)
			return err
		}

		s.log.Info("Successfully deleted shard from node", "node", ds)
	}

	mutShardMeta.PendingDeleteShardNodes = nil
	return nil
}
