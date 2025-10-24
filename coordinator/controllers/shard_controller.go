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
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
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

	chanBufferSize = 100
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

	SyncServerAddress()

	Metadata() *Metadata
	Swap(action *actions.SwapNodeAction)
	Election(action *actions.ElectionAction)
	Delete()
}

type Metadata struct {
	sync.RWMutex
	shardMetadata model.ShardMetadata
}

func (s *Metadata) Load() model.ShardMetadata {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata
}

func (s *Metadata) Compute(fn func(metadata *model.ShardMetadata)) model.ShardMetadata {
	s.Lock()
	defer s.Unlock()
	fn(&s.shardMetadata)
	return s.shardMetadata
}

func (s *Metadata) Store(metadata model.ShardMetadata) {
	s.Lock()
	defer s.Unlock()
	s.shardMetadata = metadata
}

func (s *Metadata) Leader() *model.Server {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Leader
}

func (s *Metadata) Term() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Term
}

func NewMetadata(metadata model.ShardMetadata) Metadata {
	return Metadata{
		RWMutex:       sync.RWMutex{},
		shardMetadata: metadata,
	}
}

type shardController struct {
	*slog.Logger
	sync.WaitGroup
	context.Context
	context.CancelFunc
	// metrics
	leaderElectionLatency metric.LatencyHistogram
	newTermQuorumLatency  metric.LatencyHistogram
	becomeLeaderLatency   metric.LatencyHistogram
	leaderElectionsFailed metric.Counter
	termGauge             metric.Gauge
	// resources
	leaderSelector selectors.Selector[*leaderselector.Context, model.Server]
	eventListener  ShardEventListener
	configResource resources.ClusterConfigResource
	statusResource resources.StatusResource
	provider       rpc.Provider
	// state machine operations
	electionOp    chan *actions.ElectionAction
	deleteOp      chan *actions.DeleteAction
	swapNodeOp    chan *actions.SwapNodeAction
	nodeFailureOp chan model.Server
	// owned status
	namespace       string
	shard           int64
	metadata        Metadata
	currentElection *Election // NOTICE: this must be running in the main event eventLoop
}

func (s *shardController) Metadata() *Metadata {
	return &s.metadata
}

func (s *shardController) NodeBecameUnavailable(node model.Server) {
	s.nodeFailureOp <- node
}

func NewShardController(
	namespace string,
	shard int64,
	shardMetadata model.ShardMetadata,
	configResource resources.ClusterConfigResource,
	statusResource resources.StatusResource,
	eventListener ShardEventListener,
	rpcProvider rpc.Provider) ShardController {
	labels := metric.LabelsForShard(namespace, shard)
	s := &shardController{
		Logger: slog.With(
			slog.String("component", "shard-controller"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shard),
		),
		WaitGroup:      sync.WaitGroup{},
		namespace:      namespace,
		shard:          shard,
		metadata:       NewMetadata(shardMetadata),
		provider:       rpcProvider,
		configResource: configResource,
		statusResource: statusResource,
		eventListener:  eventListener,
		leaderSelector: leaderselector.NewSelector(),
		electionOp:     make(chan *actions.ElectionAction, chanBufferSize),
		deleteOp:       make(chan *actions.DeleteAction, chanBufferSize),
		swapNodeOp:     make(chan *actions.SwapNodeAction, chanBufferSize),
		nodeFailureOp:  make(chan model.Server, chanBufferSize),
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

	s.Context, s.CancelFunc = context.WithCancel(context.Background())

	metadataSnapshot := s.metadata.Load()
	s.Info(
		"Started shard controller",
		slog.Any("shard-metadata", metadataSnapshot),
	)

	s.Add(1)
	go process.DoWithLabels(
		s.Context,
		map[string]string{
			"oxia":      "shard-controller",
			"namespace": s.namespace,
			"shard":     fmt.Sprintf("%d", s.shard),
		}, func() {
			s.eventLoop(&metadataSnapshot)
		})
	return s
}

func (s *shardController) Election(action *actions.ElectionAction) {
	s.electionOp <- action
}

func (s *shardController) Swap(action *actions.SwapNodeAction) {
	s.swapNodeOp <- action
}

func (s *shardController) Delete() {
	action := actions.DeleteAction{
		Shard:  s.shard,
		Waiter: &sync.WaitGroup{},
	}
	s.deleteOp <- &action
	action.Waiter.Wait()
}
func (s *shardController) SyncServerAddress() {
	currentMeta := s.metadata.Load()
	needSync := false
	for _, candidate := range currentMeta.Ensemble {
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
	s.Info("server address changed, start a new leader election")
	s.electionOp <- nil
}

func (s *shardController) Close() error {
	s.CancelFunc()
	// NOTE: we must wait the eventLoop goroutine to exit, otherwise
	// the controller maybe running after close is returned.
	s.Wait()
	s.termGauge.Unregister()
	if s.currentElection != nil {
		s.currentElection.Stop()
		s.currentElection = nil
	}
	return nil
}

func (s *shardController) verifyEnsemble(metadataSnapshot *model.ShardMetadata) bool {
	// Ideally, we shouldn't need to trigger a new leader election if a follower
	// is out of sync. We should just go back into the retry-to-fence follower
	// eventLoop. In practice, the current approach is easier for now.
	for _, node := range metadataSnapshot.Ensemble {
		nodeStatus, err := s.provider.GetStatus(s.Context, node, &proto.GetStatusRequest{Shard: s.shard})
		switch {
		case err != nil:
			// todo: consider quorum ok but one follower is failed
			s.Warn(
				"Failed to verify status for shard. Start a new election",
				slog.Any("error", err),
				slog.Any("node", node),
			)
			return false
		case node.GetIdentifier() == metadataSnapshot.Leader.GetIdentifier() &&
			nodeStatus.Status != proto.ServingStatus_LEADER:
			s.Warn(
				"Expected leader is not in leader status. Start a new election",
				slog.Any("node", node),
				slog.Any("status", nodeStatus.Status),
			)
			return false
		case node.GetIdentifier() != metadataSnapshot.Leader.GetIdentifier() &&
			nodeStatus.Status != proto.ServingStatus_FOLLOWER:
			s.Warn(
				"Expected follower is not in follower status. Start a new election",
				slog.Any("node", node),
				slog.Any("status", nodeStatus.Status),
			)
			return false
		case nodeStatus.Term != metadataSnapshot.Term:
			s.Warn(
				"Node has a wrong term. Start a new election",
				slog.Any("node", node),
				slog.Any("node-term", nodeStatus.Term),
				slog.Any("coordinator-term", metadataSnapshot.Term),
			)
			return false
		default:
			s.Info(
				"Node looks ok",
				slog.Any("node", node),
			)
		}
	}
	s.Info("All nodes look good. No need to trigger new leader election")
	return true
}

func (s *shardController) eventLoop(metadataSnapshot *model.ShardMetadata) {
	defer s.WaitGroup.Done()

	leader := metadataSnapshot.Leader
	status := metadataSnapshot.Status
	// Do initial check or leader election
	switch {
	case status == model.ShardStatusDeleting:
		s.deleteOp <- nil
	case leader == nil || status != model.ShardStatusSteadyState:
		electedLeader := s.election()
		leader = &electedLeader
	default:
		s.Info("There is already a node marked as leader on the shard, verifying",
			slog.Any("current-leader", leader))
		if !s.verifyEnsemble(metadataSnapshot) {
			s.election()
		} else {
			s.SyncServerAddress()
		}
	}
	s.Info("Shard is ready", slog.Any("leader", leader))

	deleteRemovedBackoff := oxiatime.NewBackOff(s.Context)
	deleteRemovedTimer := time.NewTimer(deleteRemovedBackoff.NextBackOff())
	defer deleteRemovedTimer.Stop()

	for {
		select {
		case <-s.Context.Done():
			return
		case op := <-s.deleteOp:
			currentMeta := s.metadata.Load()
			s.onDelete(slices.Concat(currentMeta.Ensemble, currentMeta.RemovedNodes))
			op.Done(nil)
		case op := <-s.nodeFailureOp:
			s.onNodeFailure(op)
		case op := <-s.swapNodeOp:
			s.onSwap(op.From, op.To)
			op.Done(nil)
		case op := <-s.electionOp:
			election := s.election()
			op.Done(election.GetIdentifier())
		case <-deleteRemovedTimer.C:
			currentMetadata := s.metadata.Load()
			if len(currentMetadata.RemovedNodes) == 0 {
				deleteRemovedTimer.Reset(deleteRemovedBackoff.NextBackOff())
				continue
			}
			// reset backoff
			deleteRemovedBackoff.Reset()
			deleteRemovedTimer.Reset(deleteRemovedBackoff.NextBackOff())
			s.onDelete(currentMetadata.RemovedNodes)
			// reset removed nodes
			newMeta := s.metadata.Compute(func(metadata *model.ShardMetadata) {
				metadata.RemovedNodes = make([]model.Server, 0)
			})
			s.statusResource.UpdateShardMetadata(s.namespace, s.shard, newMeta)
		}
	}
}

func (s *shardController) onDelete(ensemble []model.Server) {
	s.Info("Deleting shard from nodes", slog.Any("nodes", ensemble))
	_ = backoff.RetryNotify(func() error {
		for _, server := range ensemble {
			// We need to save the address because it gets modified in the eventLoop
			if _, err := s.provider.DeleteShard(s.Context, server, &proto.DeleteShardRequest{
				Namespace: s.namespace,
				Shard:     s.shard,
				Term:      s.metadata.Term(),
			}); err != nil {
				s.Warn("Failed to delete shard", slog.Any("error", err), slog.Any("node", server))
				return err
			}
			s.Info("Successfully deleted shard from node", slog.Any("server-address", server))
		}
		s.statusResource.DeleteShardMetadata(s.namespace, s.shard)
		s.eventListener.ShardDeleted(s.shard)
		return nil
	}, oxiatime.NewBackOff(s.Context), func(err error, duration time.Duration) {
		s.Logger.Warn("Delete shard failed, retrying later", slog.Duration("retry-after", duration), slog.Any("error", err))
	})
}

func (s *shardController) election() model.Server {
	// stop the current term election
	if s.currentElection != nil {
		s.currentElection.Stop()
		s.currentElection = nil
	}
	enableNotification := entity.OptBooleanDefaultTrue{}
	if nsConfig, exist := s.configResource.NamespaceConfig(s.namespace); exist {
		enableNotification = nsConfig.NotificationsEnabled
	}
	s.currentElection = CreateNewElection(s.Context, s.Logger, s.eventListener,
		s.statusResource, s.configResource, s.leaderSelector,
		s.provider, &s.metadata, s.namespace, s.shard,
		&proto.NewTermOptions{EnableNotifications: enableNotification.Get()},
		s.leaderElectionLatency,
		s.newTermQuorumLatency,
		s.becomeLeaderLatency,
		s.leaderElectionsFailed)

	leaderNode := s.currentElection.Start()
	return leaderNode
}

func (s *shardController) onNodeFailure(failedNode model.Server) {
	leader := s.metadata.Leader()
	s.Debug(
		"Received notification of failed node",
		slog.Any("failed-node", failedNode),
		slog.Any("current-leader", leader),
	)

	if leader != nil && leader.GetIdentifier() == failedNode.GetIdentifier() {
		s.Info("Detected failure on shard leader, triggering new term election", slog.Any("leader", failedNode))
		s.election()
	}
}

func (s *shardController) onSwap(from model.Server, to model.Server) {
	if s.currentElection != nil {
		s.currentElection.EnsureFollowerCaught()
	}
	newMeta := s.metadata.Compute(func(metadata *model.ShardMetadata) {
		metadata.RemovedNodes = append(metadata.RemovedNodes, from)
		filteredList := slices.DeleteFunc(metadata.Ensemble, func(s model.Server) bool {
			return s.GetIdentifier() == from.GetIdentifier()
		})
		metadata.Ensemble = append(filteredList, to)
	})
	s.Info(
		"Swapping node",
		slog.Any("removed-nodes", newMeta.RemovedNodes),
		slog.Any("new-ensemble", newMeta.Ensemble),
		slog.Any("from", from),
		slog.Any("to", to),
	)

	s.election()

	s.Info("Successfully swapped node, All the followers are caught up after node-swap",
		slog.Any("from", from), slog.Any("to", to))
}
