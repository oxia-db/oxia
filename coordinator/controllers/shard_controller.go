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

package controllers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/entity"
	"github.com/oxia-db/oxia/coordinator/actions"
	"github.com/oxia-db/oxia/coordinator/selectors"
	leaderselector "github.com/oxia-db/oxia/coordinator/selectors/leader"

	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/coordinator/rpc"

	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/proto"
)

const (
	// When fencing quorum of servers, after we reach the majority, wait a bit more
	// to include responses from all healthy servers.
	quorumFencingGracePeriod = 100 * time.Millisecond

	// Timeout when waiting for followers to catchup with leader.
	catchupTimeout = 5 * time.Minute

	chanBufferSize = 100

	DefaultPeriodicTasksInterval = 1 * time.Minute
)

type swapNodeRequest struct {
	from model.Server
	to   model.Server
	res  chan error
}

var _ ShardController = &shardController{}

// The ShardController is responsible to handle all the state transition for a given a shard
// e.g. electing a new leader.
type ShardController interface {
	io.Closer
	NodeEventListener

	Metadata() *Metadata

	SyncServerAddress()

	SwapNode(from model.Server, to model.Server) error
	DeleteShard()

	Election(action *actions.ElectionAction) string
}

type shardController struct {
	namespace       string
	shard           int64
	namespaceConfig *model.NamespaceConfig
	rpc             rpc.Provider
	metadata        Metadata

	leaderSelector selectors.Selector[*leaderselector.Context, model.Server]

	eventListener  ShardEventListener
	configResource resources.ClusterConfigResource
	statusResource resources.StatusResource

	electionOp    chan *actions.ElectionAction
	deleteOp      chan any
	nodeFailureOp chan model.Server
	swapNodeOp    chan swapNodeRequest

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
	configResource resources.ClusterConfigResource,
	statusResource resources.StatusResource,
	eventListener ShardEventListener,
	rpcProvider rpc.Provider,
	periodTasksInterval time.Duration) ShardController {
	labels := metric.LabelsForShard(namespace, shard)
	s := &shardController{
		namespace:             namespace,
		shard:                 shard,
		namespaceConfig:       nc,
		metadata:              NewMetadata(shardMetadata),
		rpc:                   rpcProvider,
		configResource:        configResource,
		statusResource:        statusResource,
		eventListener:         eventListener,
		leaderSelector:        leaderselector.NewSelector(),
		electionOp:            make(chan *actions.ElectionAction, chanBufferSize),
		deleteOp:              make(chan any, chanBufferSize),
		nodeFailureOp:         make(chan model.Server, chanBufferSize),
		swapNodeOp:            make(chan swapNodeRequest, chanBufferSize),
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

func (s *shardController) Election(action *actions.ElectionAction) string {
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
		s.electLeader()
	default:
		s.log.Info(
			"There is already a node marked as leader on the shard, verifying",
			slog.Any("current-leader", initShardMeta.Leader),
		)

		if !s.verifyCurrentEnsemble(initShardMeta) {
			s.electLeader()
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

		case sw := <-s.swapNodeOp:
			s.swapNode(sw.from, sw.to, sw.res)

		case <-periodicTasksTimer.C:
			s.handlePeriodicTasks()

		case electionAction := <-s.electionOp:
			newLeader := s.electLeader()
			electionAction.Done(newLeader)
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
		s.electLeader()
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

func (s *shardController) electLeader() model.Server {
	// stop the current term election
	if s.currentElection != nil {
		s.currentElection.Stop()
		s.currentElection = nil
	}
	enableNotification := entity.OptBooleanDefaultTrue{}
	if nsConfig, exist := s.configResource.NamespaceConfig(s.namespace); exist {
		enableNotification = nsConfig.NotificationsEnabled
	}
	s.currentElection = NewShardElection(s.ctx, s.log, s.eventListener,
		s.statusResource, s.configResource, s.leaderSelector,
		s.rpc, &s.metadata, s.namespace, s.shard,
		&proto.NewTermOptions{EnableNotifications: enableNotification.Get()},
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

func (s *shardController) SwapNode(from model.Server, to model.Server) error {
	res := make(chan error)
	s.swapNodeOp <- swapNodeRequest{
		from: from,
		to:   to,
		res:  res,
	}

	return <-res
}

func (s *shardController) swapNode(from model.Server, to model.Server, res chan error) {
	shardMeta := s.metadata.Compute(func(shardMeta *model.ShardMetadata) {
		shardMeta.RemovedNodes = listAddUnique(shardMeta.RemovedNodes, from)

		// A node might get re-added to the ensemble after it was swapped out and be in
		// pending delete state. We don't want a background task to attempt deletion anymore
		shardMeta.PendingDeleteShardNodes = listRemove(shardMeta.PendingDeleteShardNodes, to)
		shardMeta.Ensemble = replaceInList(shardMeta.Ensemble, from, to)
	})

	s.log.Info(
		"Swapping node",
		slog.Any("removed-nodes", shardMeta),
		slog.Any("new-ensemble", shardMeta),
		slog.Any("from", from),
		slog.Any("to", to),
	)

	// Wait until we can re-establish a leader with the new ensemble
	s.electLeader()

	// Reload snapshot from latest
	shardMeta = s.metadata.Load()
	// Wait until all followers are caught up.
	// This is done to avoid doing multiple node-swap concurrently, since it would create
	// additional load in the system, while transferring multiple DB snapshots.
	if err := s.waitForFollowersToCatchUp(s.ctx, *shardMeta.Leader, shardMeta.Ensemble); err != nil {
		s.log.Error(
			"Failed to wait for followers to catch up",
			slog.Any("error", err),
		)
		res <- err
		return
	}

	s.log.Info(
		"Successfully swapped node",
		slog.Any("from", from),
		slog.Any("to", to),
	)
	res <- nil
}

func (s *shardController) isFollowerCatchUp(ctx context.Context, server model.Server, leaderHeadOffset int64) error {
	fs, err := s.rpc.GetStatus(ctx, server, &proto.GetStatusRequest{Shard: s.shard})
	if err != nil {
		return err
	}

	followerHeadOffset := fs.HeadOffset
	if followerHeadOffset >= leaderHeadOffset {
		s.log.Info(
			"Follower is caught-up with the leader after node-swap",
			slog.Any("server", server),
		)
		return nil
	}

	s.log.Info(
		"Follower is *not* caught-up yet with the leader",
		slog.Any("server", server),
		slog.Int64("leader-head-offset", leaderHeadOffset),
		slog.Int64("follower-head-offset", followerHeadOffset),
	)
	return errors.New("follower not caught up yet")
}

// Check that all the followers in the ensemble are catching up with the leader.
func (s *shardController) waitForFollowersToCatchUp(ctx context.Context, leader model.Server, ensemble []model.Server) error {
	ctx, cancel := context.WithTimeout(ctx, catchupTimeout)
	defer cancel()

	// Get current head offset for leader
	ls, err := s.rpc.GetStatus(ctx, leader, &proto.GetStatusRequest{Shard: s.shard})
	if err != nil {
		return errors.Wrapf(err, "failed to get leader status from %s", leader.GetIdentifier())
	}

	leaderHeadOffset := ls.HeadOffset

	for _, server := range ensemble {
		if server.GetIdentifier() == leader.GetIdentifier() {
			continue
		}

		err = backoff.Retry(func() error {
			return s.isFollowerCatchUp(ctx, server, leaderHeadOffset)
		}, oxiatime.NewBackOff(ctx))

		if err != nil {
			return errors.Wrapf(err, "failed to get the follower status from %s", server.GetIdentifier())
		}
	}

	s.log.Info("All the followers are caught up after node-swap")
	return nil
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
	s.electionOp <- nil
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

// listAddUnique Adds a server to the list if it's not already there.
func listAddUnique(list []model.Server, sa model.Server) []model.Server {
	if !listContains(list, sa) {
		list = append(list, sa)
	}
	return list
}

func listRemove(list []model.Server, toRemove model.Server) []model.Server {
	for i, item := range list {
		if item.GetIdentifier() == toRemove.GetIdentifier() {
			return append(list[:i], list[i+1:]...)
		}
	}

	return list
}

func listContains(list []model.Server, sa model.Server) bool {
	for _, item := range list {
		if item.GetIdentifier() == sa.GetIdentifier() {
			return true
		}
	}

	return false
}

func replaceInList(list []model.Server, oldServer, newServer model.Server) []model.Server {
	var res []model.Server
	for _, item := range list {
		if item.GetIdentifier() != oldServer.GetIdentifier() {
			res = append(res, item)
		}
	}

	res = append(res, newServer)
	return res
}
