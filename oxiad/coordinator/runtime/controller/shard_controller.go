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

	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/leader"

	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
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
	DataServerEventListener

	Metadata() *Metadata

	SyncServerAddress()

	DeleteShard()

	Election(electionAction *action.ElectionAction) string

	ChangeEnsemble(changeEnsembleAction *action.ChangeEnsembleAction)
}

type DataServerSupportedFeaturesSupplier = func(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature

func NoOpSupportedFeaturesSupplier([]*proto.DataServerIdentity) map[string][]proto.Feature {
	return map[string][]proto.Feature{}
}

type shardController struct {
	namespace       string
	shard           int64
	namespaceConfig *proto.Namespace
	rpc             rpc.Provider
	metadata        Metadata

	leaderSelector selector.Selector[*leaderselector.Context, *proto.DataServerIdentity]

	eventListener                       ShardEventListener
	metadataStore                       coordmetadata.Metadata
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier

	electionOp          chan *action.ElectionAction
	deleteOp            chan any
	dataServerFailureOp chan *proto.DataServerIdentity
	changeEnsembleOp    chan *action.ChangeEnsembleAction

	ctx                   context.Context
	ctxCancel             context.CancelFunc
	wg                    sync.WaitGroup
	periodicTasksInterval time.Duration
	logger                *slog.Logger

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

func (s *shardController) BecameUnavailable(dataServer *proto.DataServerIdentity) {
	s.dataServerFailureOp <- dataServer
}

//nolint:revive
func NewShardController(
	namespace string,
	shard int64,
	nc *proto.Namespace,
	shardMetadata *proto.ShardMetadata,
	metadataStore coordmetadata.Metadata,
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier,
	eventListener ShardEventListener,
	rpcProvider rpc.Provider,
	periodTasksInterval time.Duration) ShardController {
	labels := metric.LabelsForShard(namespace, shard)
	s := &shardController{
		namespace:                           namespace,
		shard:                               shard,
		namespaceConfig:                     nc,
		metadata:                            NewMetadata(shardMetadata),
		rpc:                                 rpcProvider,
		metadataStore:                       metadataStore,
		dataServerSupportedFeaturesSupplier: dataServerSupportedFeaturesSupplier,
		eventListener:                       eventListener,
		leaderSelector:                      leaderselector.NewSelector(),
		electionOp:                          make(chan *action.ElectionAction, chanBufferSize),
		deleteOp:                            make(chan any, chanBufferSize),
		dataServerFailureOp:                 make(chan *proto.DataServerIdentity, chanBufferSize),
		changeEnsembleOp:                    make(chan *action.ChangeEnsembleAction, chanBufferSize),

		periodicTasksInterval: periodTasksInterval,
		logger: slog.With(
			slog.String("component", "shard-controller"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shard),
		),
		wg: sync.WaitGroup{},

		leaderElectionLatency: metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		leaderElectionsFailed: metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels),
		newTermQuorumLatency: metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		becomeLeaderLatency: metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
	}

	s.termGauge = metric.NewGauge("oxia_coordinator_term",
		"The term of the shard", "count", labels, func() int64 {
			return s.metadata.Term()
		})

	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	shardMeta := s.metadata.Load()
	s.logger.Info("Started shard controller", slog.Any("shard-metadata", shardMeta))

	s.wg.Go(func() {
		process.DoWithLabels(
			s.ctx,
			map[string]string{
				"oxia":      "shard-controller",
				"namespace": s.namespace,
				"shard":     fmt.Sprintf("%d", s.shard),
			}, func() {
				s.run(shardMeta)
			},
		)
	})

	return s
}

func (s *shardController) Election(electionAction *action.ElectionAction) string {
	clonedAction := electionAction.Clone()
	clonedAction.Waiter.Add(1)
	s.electionOp <- clonedAction
	clonedAction.Waiter.Wait()
	return clonedAction.NewLeader
}

func (s *shardController) run(initShardMeta *proto.ShardMetadata) {
	// Do initial check or leader election
	switch {
	case initShardMeta.GetStatusOrDefault() == proto.ShardStatusDeleting:
		s.DeleteShard()
	case initShardMeta.Split != nil && len(initShardMeta.Split.ChildShardIds) == 0:
		// Child shard during a split: the SplitController manages its lifecycle.
		// Wait until the split is complete (Split metadata cleared) before
		// entering the normal event loop, to prevent the load balancer from
		// triggering elections that would interfere with the split controller.
		s.logger.Info("Child shard during split, waiting for split to complete")
		s.waitForSplitComplete()
	case initShardMeta.Leader == nil || initShardMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState:
		s.onElectLeader(nil)
	default:
		s.logger.Info(
			"There is already a data server marked as leader on the shard, verifying",
			slog.Any("current-leader", initShardMeta.Leader),
		)

		if !s.verifyCurrentEnsemble(initShardMeta) {
			s.onElectLeader(nil)
		} else {
			s.SyncServerAddress()
		}
	}

	s.logger.Info("Shard is ready", slog.Any("leader", initShardMeta.Leader))

	periodicTasksTimer := time.NewTicker(s.periodicTasksInterval)

	for {
		select {
		case <-s.ctx.Done():
			return

		case <-s.deleteOp:
			s.deleteShardWithRetries()
		case n := <-s.dataServerFailureOp:
			s.handleDataServerFailure(n)
		case op := <-s.changeEnsembleOp:
			s.onChangeEnsemble(op)
		case <-periodicTasksTimer.C:
			s.handlePeriodicTasks()
		case electionAction := <-s.electionOp:
			newLeader := s.onElectLeader(nil)
			electionAction.Done(newLeader.GetNameOrDefault())
		}
	}
}

// waitForSplitComplete blocks until the Split metadata is cleared from this
// shard in the status resource, indicating the split controller has finished
// and the shard can operate normally. This prevents the load balancer from
// triggering elections that would interfere with the split controller.
// We read from the status resource (not the shard controller's local metadata)
// because the split controller updates the status resource directly.
func (s *shardController) waitForSplitComplete() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			status := s.metadataStore.GetStatus().UnsafeBorrow()
			ns, exists := status.Namespaces[s.namespace]
			if !exists {
				continue
			}
			meta, exists := ns.Shards[s.shard]
			if !exists {
				continue
			}
			if meta.Split == nil {
				s.logger.Info("Split complete, child shard entering normal operation",
					slog.Any("leader", meta.Leader),
				)
				// Update local metadata to match the status resource
				s.metadata.Store(meta)
				return
			}
		}
	}
}

func (s *shardController) handleDataServerFailure(failedDataServer *proto.DataServerIdentity) {
	shardMeta := s.metadata.Load()
	s.logger.Debug(
		"Received notification of failed data server",
		slog.Any("failed-data-server", failedDataServer.GetNameOrDefault()),
		slog.Any("current-leader", shardMeta.Leader),
	)

	if shardMeta.Leader != nil &&
		shardMeta.Leader.GetNameOrDefault() == failedDataServer.GetNameOrDefault() {
		s.logger.Info(
			"Detected failure on shard leader",
			slog.Any("leader", failedDataServer.GetNameOrDefault()),
		)
		s.onElectLeader(nil)
	}
}

func (s *shardController) verifyCurrentEnsemble(initShardMeta *proto.ShardMetadata) bool {
	// Ideally, we shouldn't need to trigger a new leader election if a follower
	// is out of sync. We should just go back into the retry-to-fence follower
	// loop. In practice, the current approach is easier for now.
	for _, dataServer := range initShardMeta.Ensemble {
		dataServerStatus, err := s.rpc.GetStatus(s.ctx, dataServer, &proto.GetStatusRequest{Shard: s.shard})

		switch {
		case err != nil:
			s.logger.Warn(
				"Failed to verify status for shard. Start a new election",
				slog.Any("error", err),
				slog.Any("data-server", dataServer),
			)
			return false
		case dataServer.GetNameOrDefault() == initShardMeta.Leader.GetNameOrDefault() &&
			dataServerStatus.Status != proto.ServingStatus_LEADER:
			s.logger.Warn(
				"Expected leader is not in leader status. Start a new election",
				slog.Any("data-server", dataServer),
				slog.Any("status", dataServerStatus.Status),
			)
			return false
		case dataServer.GetNameOrDefault() != initShardMeta.Leader.GetNameOrDefault() &&
			dataServerStatus.Status != proto.ServingStatus_FOLLOWER:
			s.logger.Warn(
				"Expected follower is not in follower status. Start a new election",
				slog.Any("data-server", dataServer),
				slog.Any("status", dataServerStatus.Status),
			)
			return false
		case dataServerStatus.Term != initShardMeta.Term:
			s.logger.Warn(
				"Node has a wrong term. Start a new election",
				slog.Any("data-server", dataServer),
				slog.Any("data-server-term", dataServerStatus.Term),
				slog.Any("coordinator-term", initShardMeta.Term),
			)
			return false
		default:
			s.logger.Info(
				"Data Server looks ok",
				slog.Any("data-server", dataServer),
			)
		}
	}

	s.logger.Info("All data servers look good. No need to trigger new leader election")
	return true
}

func (s *shardController) onElectLeader(changeEnsembleAction *action.ChangeEnsembleAction) *proto.DataServerIdentity {
	// stop the current term election
	if s.currentElection != nil {
		s.currentElection.Stop()
		s.currentElection = nil
	}
	termOptions := &proto.NewTermOptions{
		EnableNotifications: true,
		KeySorting:          proto.KeySortingType_UNKNOWN,
	}
	borrowedNamespaceConfig, exist := s.metadataStore.GetNamespace(s.namespace)
	if exist {
		nsConfig := borrowedNamespaceConfig.UnsafeBorrow()
		termOptions.EnableNotifications = nsConfig.NotificationsEnabledOrDefault()
		termOptions.KeySorting, _ = nsConfig.GetKeySortingType()
	}
	s.currentElection = NewShardElection(s.ctx, s.logger, s.eventListener,
		s.metadataStore, s.dataServerSupportedFeaturesSupplier, s.leaderSelector,
		s.rpc, &s.metadata, s.namespace, s.shard, changeEnsembleAction,
		termOptions,
		s.leaderElectionLatency,
		s.newTermQuorumLatency,
		s.becomeLeaderLatency,
		s.leaderElectionsFailed)
	leaderDataServer := s.currentElection.Start()
	return leaderDataServer
}

func (s *shardController) deleteShardRpc(ctx context.Context, dataServer *proto.DataServerIdentity) error {
	_, err := s.rpc.DeleteShard(ctx, dataServer, &proto.DeleteShardRequest{
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
	s.logger.Info("Deleting shard")

	_ = backoff.RetryNotify(s.deleteShard, oxiatime.NewBackOff(s.ctx),
		func(err error, duration time.Duration) {
			s.logger.Warn(
				"Delete shard failed, retrying later",
				slog.Duration("retry-after", duration),
				slog.Any("error", err),
			)
		})

	s.ctxCancel()
}

func (s *shardController) deleteShard() error {
	shardMeta := s.metadata.Load()
	for _, server := range shardMeta.Ensemble {
		// We need to save the address because it gets modified in the loop
		if err := s.deleteShardRpc(s.ctx, server); err != nil {
			s.logger.Warn(
				"Failed to delete shard",
				slog.Any("error", err),
				slog.Any("data-server", server),
			)
			return err
		}

		s.logger.Info(
			"Successfully deleted shard from data server",
			slog.Any("server-address", server),
		)
	}

	s.metadataStore.DeleteShardStatus(s.namespace, s.shard)
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
	s.ctxCancel()
	s.termGauge.Unregister()
	return nil
}

func (s *shardController) ChangeEnsemble(changeEnsembleAction *action.ChangeEnsembleAction) {
	s.changeEnsembleOp <- changeEnsembleAction
}

func (s *shardController) onChangeEnsemble(changeEnsembleAction *action.ChangeEnsembleAction) {
	var err error
	defer func() {
		if err != nil {
			changeEnsembleAction.Error(err)
		} else {
			changeEnsembleAction.Done(nil)
		}
	}()
	if s.currentElection != nil {
		if ready := s.currentElection.IsReadyForChangeEnsemble(); !ready {
			s.logger.Warn("Change ensemble rejected: shard is not ready (follower catch-up still in progress)")
			err = ErrNotReadyForChangeEnsemble
			return
		}
	}
	// todo: support optimized ensemble change to avoid start a new election
	s.onElectLeader(changeEnsembleAction)
}
func (s *shardController) SyncServerAddress() {
	shardMeta := s.metadata.Load()
	needSync := false
	for _, candidate := range shardMeta.Ensemble {
		if borrowedDataServer, ok := s.metadataStore.GetDataServer(candidate.GetNameOrDefault()); ok {
			newInfo := borrowedDataServer.UnsafeBorrow().GetIdentity()
			if newInfo != nil &&
				(newInfo.GetPublic() != candidate.GetPublic() || newInfo.GetInternal() != candidate.GetInternal()) {
				needSync = true
				break
			}
		}
	}
	if !needSync {
		return
	}
	s.logger.Info("server address changed, start a new leader election")
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
		if err = s.handlePendingDeleteShard(mutShardMeta); err != nil {
			s.logger.Warn("Failed to handle pending delete shard", "error", err)
			return
		}
	}

	// Update the shard status
	s.metadataStore.UpdateShardStatus(s.namespace, s.shard, mutShardMeta)
	s.metadata.Store(mutShardMeta)
}

func (s *shardController) handlePendingDeleteShard(mutShardMeta *proto.ShardMetadata) error {
	for _, ds := range mutShardMeta.PendingDeleteShardNodes {
		s.logger.Info("Deleting shard from removed data server", slog.Any("data-server", ds))

		if _, err := s.rpc.DeleteShard(s.ctx, ds, &proto.DeleteShardRequest{
			Namespace: s.namespace,
			Shard:     s.shard,
			Term:      mutShardMeta.Term,
		}); err != nil {
			s.logger.Warn("Failed to delete shard from removed data server", slog.Any("data-server", ds), slog.Any("error", err))
			return err
		}

		s.logger.Info("Successfully deleted shard from data server", slog.Any("data-server", ds))
	}

	mutShardMeta.PendingDeleteShardNodes = nil
	return nil
}
