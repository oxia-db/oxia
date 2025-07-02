// Copyright 2023 StreamNative, Inc.
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
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/coordinator/actions"
	"github.com/oxia-db/oxia/coordinator/selectors"
	leaderselector "github.com/oxia-db/oxia/coordinator/selectors/leader"

	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/coordinator/rpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

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
)

type swapNodeRequest struct {
	from model.Server
	to   model.Server
	res  chan error
}

type newTermAndAddFollowerRequest struct {
	ctx  context.Context
	node model.Server
	res  chan error
}

var _ ShardController = &shardController{}

// The ShardController is responsible to handle all the state transition for a given a shard
// e.g. electing a new leader.
type ShardController interface {
	io.Closer
	NodeEventListener

	SyncServerAddress()

	SwapNode(from model.Server, to model.Server) error
	DeleteShard()

	Election(action *actions.ElectionAction) string

	Term() int64
	Leader() *model.Server
	Status() model.ShardStatus
}

type shardController struct {
	namespace          string
	shard              int64
	namespaceConfig    *model.NamespaceConfig
	shardMetadata      model.ShardMetadata
	shardMetadataMutex sync.RWMutex
	rpc                rpc.Provider

	leaderSelector selectors.Selector[*leaderselector.Context, model.Server]

	eventListener  ShardEventListener
	configResource resources.ClusterConfigResource
	statusResource resources.StatusResource

	electionOp              chan *actions.ElectionAction
	deleteOp                chan any
	nodeFailureOp           chan model.Server
	swapNodeOp              chan swapNodeRequest
	newTermAndAddFollowerOp chan newTermAndAddFollowerRequest

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	currentElectionCtx    context.Context
	currentElectionCancel context.CancelFunc
	log                   *slog.Logger

	leaderElectionLatency metric.LatencyHistogram
	newTermQuorumLatency  metric.LatencyHistogram
	becomeLeaderLatency   metric.LatencyHistogram
	leaderElectionsFailed metric.Counter
	termGauge             metric.Gauge
}

func (s *shardController) NodeBecameUnavailable(node model.Server) {
	s.nodeFailureOp <- node
}

func NewShardController(
	namespace string,
	shard int64,
	nc *model.NamespaceConfig,
	shardMetadata model.ShardMetadata,
	configResource resources.ClusterConfigResource,
	statusResource resources.StatusResource,
	eventListener ShardEventListener,
	rpcProvider rpc.Provider) ShardController {
	labels := metric.LabelsForShard(namespace, shard)
	s := &shardController{
		namespace:               namespace,
		shard:                   shard,
		namespaceConfig:         nc,
		shardMetadata:           shardMetadata,
		rpc:                     rpcProvider,
		configResource:          configResource,
		statusResource:          statusResource,
		eventListener:           eventListener,
		leaderSelector:          leaderselector.NewSelector(),
		electionOp:              make(chan *actions.ElectionAction, chanBufferSize),
		deleteOp:                make(chan any, chanBufferSize),
		nodeFailureOp:           make(chan model.Server, chanBufferSize),
		swapNodeOp:              make(chan swapNodeRequest, chanBufferSize),
		newTermAndAddFollowerOp: make(chan newTermAndAddFollowerRequest, chanBufferSize),
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
			return s.shardMetadata.Term
		})

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.log.Info(
		"Started shard controller",
		slog.Any("shard-metadata", s.shardMetadata),
	)

	s.wg.Add(1)
	go process.DoWithLabels(
		s.ctx,
		map[string]string{
			"oxia":      "shard-controller",
			"namespace": s.namespace,
			"shard":     fmt.Sprintf("%d", s.shard),
		}, s.run,
	)

	return s
}

func (s *shardController) Election(action *actions.ElectionAction) string {
	clonedAction := action.Clone()
	clonedAction.Waiter.Add(1)
	s.electionOp <- clonedAction
	clonedAction.Waiter.Wait()
	return clonedAction.NewLeader
}

func (s *shardController) run() {
	defer s.wg.Done()
	// Do initial check or leader election
	switch {
	case s.shardMetadata.Status == model.ShardStatusDeleting:
		s.DeleteShard()
	case s.shardMetadata.Leader == nil || s.shardMetadata.Status != model.ShardStatusSteadyState:
		s.electLeaderWithRetries(nil)
	default:
		s.log.Info(
			"There is already a node marked as leader on the shard, verifying",
			slog.Any("current-leader", s.shardMetadata.Leader),
		)

		if !s.verifyCurrentEnsemble() {
			s.electLeaderWithRetries(nil)
		} else {
			s.SyncServerAddress()
		}
	}

	s.log.Info(
		"Shard is ready",
		slog.Any("leader", s.shardMetadata.Leader),
	)

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

		case a := <-s.newTermAndAddFollowerOp:
			s.internalNewTermAndAddFollower(a.ctx, a.node, a.res)

		case eo := <-s.electionOp:
			s.electLeaderWithRetries(eo)
		}
	}
}

func (s *shardController) handleNodeFailure(failedNode model.Server) {
	s.log.Debug(
		"Received notification of failed node",
		slog.Any("failed-node", failedNode),
		slog.Any("current-leader", s.shardMetadata.Leader),
	)

	if s.shardMetadata.Leader != nil &&
		s.shardMetadata.Leader.GetIdentifier() == failedNode.GetIdentifier() {
		s.log.Info(
			"Detected failure on shard leader",
			slog.Any("leader", failedNode),
		)
		s.electLeaderWithRetries(nil)
	}
}

func (s *shardController) verifyCurrentEnsemble() bool {
	// Ideally, we shouldn't need to trigger a new leader election if a follower
	// is out of sync. We should just go back into the retry-to-fence follower
	// loop. In practice, the current approach is easier for now.
	for _, node := range s.shardMetadata.Ensemble {
		nodeStatus, err := s.rpc.GetStatus(s.ctx, node, &proto.GetStatusRequest{Shard: s.shard})

		switch {
		case err != nil:
			s.log.Warn(
				"Failed to verify status for shard. Start a new election",
				slog.Any("error", err),
				slog.Any("node", node),
			)
			return false
		case node.GetIdentifier() == s.shardMetadata.Leader.GetIdentifier() &&
			nodeStatus.Status != proto.ServingStatus_LEADER:
			s.log.Warn(
				"Expected leader is not in leader status. Start a new election",
				slog.Any("node", node),
				slog.Any("status", nodeStatus.Status),
			)
			return false
		case node.GetIdentifier() != s.shardMetadata.Leader.GetIdentifier() &&
			nodeStatus.Status != proto.ServingStatus_FOLLOWER:
			s.log.Warn(
				"Expected follower is not in follower status. Start a new election",
				slog.Any("node", node),
				slog.Any("status", nodeStatus.Status),
			)
			return false
		case nodeStatus.Term != s.shardMetadata.Term:
			s.log.Warn(
				"Node has a wrong term. Start a new election",
				slog.Any("node", node),
				slog.Any("node-term", nodeStatus.Term),
				slog.Any("coordinator-term", s.shardMetadata.Term),
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

func (s *shardController) electLeaderWithRetries(ea *actions.ElectionAction) {
	newLeader, _ := backoff.RetryNotifyWithData[string](func() (string, error) {
		return s.electLeader()
	}, time2.NewBackOff(s.ctx),
		func(err error, duration time.Duration) {
			s.leaderElectionsFailed.Inc()
			s.log.Warn(
				"Leader election has failed, retrying later",
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
	if ea != nil {
		ea.Done(newLeader)
	}
}

func (s *shardController) electLeader() (string, error) {
	timer := s.leaderElectionLatency.Timer()

	if s.currentElectionCancel != nil {
		// Cancel any pending activity from the previous election
		s.currentElectionCancel()
	}

	s.currentElectionCtx, s.currentElectionCancel = context.WithCancel(s.ctx)

	s.shardMetadataMutex.Lock()
	s.shardMetadata.Status = model.ShardStatusElection
	s.shardMetadata.Leader = nil
	s.shardMetadata.Term++
	// it's a safe point to update the service info
	s.shardMetadata.Ensemble = s.getRefreshedEnsemble()
	s.shardMetadataMutex.Unlock()

	s.log.Info(
		"Starting leader election",
		slog.Int64("term", s.shardMetadata.Term),
	)

	s.statusResource.UpdateShardMetadata(s.namespace, s.shard, s.shardMetadata)

	// Send NewTerm to all the ensemble members
	fr, err := s.newTermQuorum()
	if err != nil {
		return "", err
	}

	newLeader, followers := s.selectNewLeader(fr)

	if s.log.Enabled(context.Background(), slog.LevelInfo) {
		f := make([]struct {
			ServerAddress model.Server   `json:"server-address"`
			EntryId       *proto.EntryId `json:"entry-id"`
		}, 0)
		for sa, entryId := range followers {
			f = append(f, struct {
				ServerAddress model.Server   `json:"server-address"`
				EntryId       *proto.EntryId `json:"entry-id"`
			}{ServerAddress: sa, EntryId: entryId})
		}
		s.log.Info(
			"Successfully moved ensemble to a new term",
			slog.Int64("term", s.shardMetadata.Term),
			slog.Any("new-leader", newLeader),
			slog.Any("followers", f),
		)
	}

	if err = s.becomeLeader(newLeader, followers); err != nil {
		return "", err
	}

	metadata := s.shardMetadata.Clone()
	metadata.Status = model.ShardStatusSteadyState
	metadata.Leader = &newLeader

	if len(metadata.RemovedNodes) > 0 {
		if err = s.deletingRemovedNodes(); err != nil {
			return "", err
		}

		metadata.RemovedNodes = nil
	}

	s.statusResource.UpdateShardMetadata(s.namespace, s.shard, metadata)

	s.shardMetadataMutex.Lock()
	s.shardMetadata = metadata
	s.shardMetadataMutex.Unlock()

	s.log.Info(
		"Elected new leader",
		slog.Int64("term", s.shardMetadata.Term),
		slog.Any("leader", s.shardMetadata.Leader),
	)
	timer.Done()

	if s.eventListener != nil {
		s.eventListener.LeaderElected(s.shard, newLeader, maps.Keys(followers))
	}

	s.keepFencingFailedFollowers(followers)
	return newLeader.GetIdentifier(), nil
}

func (s *shardController) getRefreshedEnsemble() []model.Server {
	currentEnsemble := s.shardMetadata.Ensemble
	refreshedEnsembleServiceAddress := make([]model.Server, len(currentEnsemble))
	for idx, candidate := range currentEnsemble {
		if refreshedAddress, exist := s.configResource.Node(candidate.GetIdentifier()); exist {
			refreshedEnsembleServiceAddress[idx] = *refreshedAddress
			continue
		}
		refreshedEnsembleServiceAddress[idx] = candidate
	}
	if s.log.Enabled(s.ctx, slog.LevelDebug) {
		if !reflect.DeepEqual(currentEnsemble, refreshedEnsembleServiceAddress) {
			s.log.Info("refresh the shard ensemble server address", slog.Any("current-ensemble", currentEnsemble),
				slog.Any("new-ensemble", refreshedEnsembleServiceAddress))
		}
	}
	return refreshedEnsembleServiceAddress
}

func (s *shardController) deletingRemovedNodes() error {
	for _, ds := range s.shardMetadata.RemovedNodes {
		if _, err := s.rpc.DeleteShard(s.ctx, ds, &proto.DeleteShardRequest{
			Namespace: s.namespace,
			Shard:     s.shard,
			Term:      s.shardMetadata.Term,
		}); err != nil {
			return err
		}

		s.log.Info(
			"Successfully deleted shard",
			slog.Any("server", ds),
		)
	}

	return nil
}

func (s *shardController) keepFencingFailedFollowers(successfulFollowers map[model.Server]*proto.EntryId) {
	if len(successfulFollowers) == len(s.shardMetadata.Ensemble)-1 {
		s.log.Debug(
			"All the member of the ensemble were successfully added",
			slog.Int64("term", s.shardMetadata.Term),
		)
		return
	}

	// Identify failed followers
	for _, sa := range s.shardMetadata.Ensemble {
		if sa == *s.shardMetadata.Leader {
			continue
		}

		if _, found := successfulFollowers[sa]; found {
			continue
		}

		s.keepFencingFollower(s.currentElectionCtx, sa)
	}
}

func (s *shardController) keepFencingFollower(ctx context.Context, node model.Server) {
	s.log.Info(
		"Node has failed in leader election, retrying",
		slog.Any("follower", node),
	)

	go process.DoWithLabels(
		s.ctx,
		map[string]string{
			"oxia":     "shard-controller-retry-failed-follower",
			"shard":    fmt.Sprintf("%d", s.shard),
			"follower": node.GetIdentifier(),
		},
		func() {
			backOff := time2.NewBackOffWithInitialInterval(ctx, 1*time.Second)

			_ = backoff.RetryNotify(func() error {
				err := s.newTermAndAddFollower(ctx, node)
				if status.Code(err) == constant.CodeInvalidTerm {
					// If we're receiving invalid term error, it would mean
					// there's already a new term generated, and we don't have
					// to keep trying with this old term
					s.log.Warn(
						"Failed to newTerm, invalid term. Stop trying",
						slog.Any("follower", node),
						slog.Int64("term", s.Term()),
					)
					return nil
				}
				return err
			}, backOff, func(err error, duration time.Duration) {
				s.log.Warn(
					"Failed to newTerm, retrying later",
					slog.Any("error", err),
					slog.Any("follower", node),
					slog.Int64("term", s.Term()),
					slog.Duration("retry-after", duration),
				)
			})
		},
	)
}

func (s *shardController) newTermAndAddFollower(ctx context.Context, node model.Server) error {
	res := make(chan error)
	s.newTermAndAddFollowerOp <- newTermAndAddFollowerRequest{
		ctx:  ctx,
		node: node,
		res:  res,
	}

	return <-res
}

func (s *shardController) internalNewTermAndAddFollower(ctx context.Context, node model.Server, res chan error) {
	fr, err := s.newTerm(ctx, node)
	if err != nil {
		res <- err
		return
	}

	leader := s.shardMetadata.Leader
	if leader == nil {
		res <- errors.New("not leader is active on the shard")
		return
	}

	if err = s.addFollower(*s.shardMetadata.Leader, node.Internal, &proto.EntryId{
		Term:   fr.Term,
		Offset: fr.Offset,
	}); err != nil {
		res <- err
		return
	}

	s.log.Info(
		"Successfully rejoined the quorum",
		slog.Any("follower", node),
		slog.Int64("term", fr.Term),
	)

	res <- nil
}

// Send NewTerm to all the ensemble members in parallel and wait for
// a majority of them to reply successfully.
func (s *shardController) newTermQuorum() (map[model.Server]*proto.EntryId, error) {
	timer := s.newTermQuorumLatency.Timer()

	fencingQuorum := mergeLists(s.shardMetadata.Ensemble, s.shardMetadata.RemovedNodes)
	fencingQuorumSize := len(fencingQuorum)
	majority := fencingQuorumSize/2 + 1

	// Use a new context, so we can cancel the pending requests
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Channel to receive responses or errors from each server
	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, fencingQuorumSize)

	for _, server := range fencingQuorum {
		// We need to save the address because it gets modified in the loop
		pinedServer := server
		go process.DoWithLabels(
			s.ctx,
			map[string]string{
				"oxia":  "shard-controller-leader-election",
				"shard": fmt.Sprintf("%d", s.shard),
				"node":  pinedServer.GetIdentifier(),
			}, func() {
				entryId, err := s.newTerm(ctx, pinedServer)
				if err != nil {
					s.log.Warn(
						"Failed to newTerm node",
						slog.Any("error", err),
						slog.Any("node", pinedServer),
					)
				} else {
					s.log.Info(
						"Processed newTerm response",
						slog.Any("node", pinedServer),
						slog.Any("entry-id", entryId),
					)
				}

				ch <- struct {
					model.Server
					*proto.EntryId
					error
				}{pinedServer, entryId, err}
			},
		)
	}

	successResponses := 0
	totalResponses := 0

	res := make(map[model.Server]*proto.EntryId)
	var err error

	// Wait for a majority to respond
	for successResponses < majority && totalResponses < fencingQuorumSize {
		r := <-ch

		totalResponses++
		if r.error == nil {
			successResponses++

			// We don't consider the removed nodes as candidates for leader/followers
			if listContains(s.shardMetadata.Ensemble, r.Server) {
				res[r.Server] = r.EntryId
			}
		} else {
			err = multierr.Append(err, r.error)
		}
	}

	if successResponses < majority {
		return nil, errors.Wrap(err, "failed to newTerm shard")
	}

	// If we have already reached a quorum of successful responses, we can wait a
	// tiny bit more, to allow time for all the "healthy" nodes to respond.
	for err == nil && totalResponses < fencingQuorumSize {
		select {
		case r := <-ch:
			totalResponses++
			if r.error == nil {
				res[r.Server] = r.EntryId
			} else {
				err = multierr.Append(err, r.error)
			}

		case <-time.After(quorumFencingGracePeriod):
			timer.Done()
			return res, nil
		}
	}

	timer.Done()
	return res, nil
}

func (s *shardController) newTerm(ctx context.Context, node model.Server) (*proto.EntryId, error) {
	res, err := s.rpc.NewTerm(ctx, node, &proto.NewTermRequest{
		Namespace: s.namespace,
		Shard:     s.shard,
		Term:      s.shardMetadata.Term,
		Options: &proto.NewTermOptions{
			EnableNotifications: s.namespaceConfig.NotificationsEnabled.Get(),
		},
	})
	if err != nil {
		return nil, err
	}

	return res.HeadEntryId, nil
}

func (s *shardController) deleteShardRpc(ctx context.Context, node model.Server) error {
	_, err := s.rpc.DeleteShard(ctx, node, &proto.DeleteShardRequest{
		Namespace: s.namespace,
		Shard:     s.shard,
		Term:      s.shardMetadata.Term,
	})

	return err
}

func chooseCandidates(newTermResponses map[model.Server]*proto.EntryId) []model.Server {
	// Select all the nodes that have the highest term first
	var currentMaxTerm int64 = -1
	// Select all the nodes that have the highest entry in the wal
	var currentMax int64 = -1
	var candidates []model.Server

	for addr, headEntryId := range newTermResponses {
		if headEntryId.Term > currentMaxTerm {
			// the new max
			currentMaxTerm = headEntryId.Term
			currentMax = headEntryId.Offset
			candidates = []model.Server{addr}
		} else if headEntryId.Term == currentMaxTerm {
			if headEntryId.Offset > currentMax {
				// the new max
				currentMax = headEntryId.Offset
				candidates = []model.Server{addr}
			} else if headEntryId.Offset == currentMax {
				candidates = append(candidates, addr)
			}
		}
	}
	return candidates
}

func (s *shardController) selectNewLeader(newTermResponses map[model.Server]*proto.EntryId) (
	leader model.Server, followers map[model.Server]*proto.EntryId) {
	candidates := chooseCandidates(newTermResponses)

	server, _ := s.leaderSelector.Select(&leaderselector.Context{
		Candidates: candidates,
		Status:     s.statusResource.Load(),
	})
	leader = server
	followers = make(map[model.Server]*proto.EntryId)
	for a, e := range newTermResponses {
		if a != leader {
			followers[a] = e
		}
	}
	return leader, followers
}

func (s *shardController) becomeLeader(leader model.Server, followers map[model.Server]*proto.EntryId) error {
	timer := s.becomeLeaderLatency.Timer()

	followersMap := make(map[string]*proto.EntryId)
	for server, e := range followers {
		followersMap[server.Internal] = e
	}

	if _, err := s.rpc.BecomeLeader(s.ctx, leader, &proto.BecomeLeaderRequest{
		Namespace:         s.namespace,
		Shard:             s.shard,
		Term:              s.shardMetadata.Term,
		ReplicationFactor: uint32(len(s.shardMetadata.Ensemble)),
		FollowerMaps:      followersMap,
	}); err != nil {
		return err
	}

	timer.Done()
	return nil
}

func (s *shardController) addFollower(leader model.Server, follower string, followerHeadEntryId *proto.EntryId) error {
	if _, err := s.rpc.AddFollower(s.ctx, leader, &proto.AddFollowerRequest{
		Namespace:           s.namespace,
		Shard:               s.shard,
		Term:                s.shardMetadata.Term,
		FollowerName:        follower,
		FollowerHeadEntryId: followerHeadEntryId,
	}); err != nil {
		return err
	}

	return nil
}

func (s *shardController) DeleteShard() {
	s.deleteOp <- nil
}

func (s *shardController) deleteShardWithRetries() {
	s.log.Info("Deleting shard")

	_ = backoff.RetryNotify(s.deleteShard, time2.NewBackOff(s.ctx),
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
	for _, server := range s.shardMetadata.Ensemble {
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

func (s *shardController) Term() int64 {
	s.shardMetadataMutex.Lock()
	defer s.shardMetadataMutex.Unlock()
	return s.shardMetadata.Term
}

func (s *shardController) Leader() *model.Server {
	s.shardMetadataMutex.Lock()
	defer s.shardMetadataMutex.Unlock()
	return s.shardMetadata.Leader
}

func (s *shardController) Status() model.ShardStatus {
	s.shardMetadataMutex.Lock()
	defer s.shardMetadataMutex.Unlock()
	return s.shardMetadata.Status
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
	s.shardMetadataMutex.Lock()
	s.shardMetadata.RemovedNodes = append(s.shardMetadata.RemovedNodes, from)
	s.shardMetadata.Ensemble = replaceInList(s.shardMetadata.Ensemble, from, to)
	s.shardMetadataMutex.Unlock()

	s.log.Info(
		"Swapping node",
		slog.Any("removed-nodes", s.shardMetadata.RemovedNodes),
		slog.Any("new-ensemble", s.shardMetadata.Ensemble),
		slog.Any("from", from),
		slog.Any("to", to),
	)
	if _, err := s.electLeader(); err != nil {
		res <- err
		return
	}

	leader := s.shardMetadata.Leader
	ensemble := s.shardMetadata.Ensemble
	ctx := s.currentElectionCtx

	// Wait until all followers are caught up.
	// This is done to avoid doing multiple node-swap concurrently, since it would create
	// additional load in the system, while transferring multiple DB snapshots.
	if err := s.waitForFollowersToCatchUp(ctx, *leader, ensemble); err != nil {
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
		return errors.Wrap(err, "failed to get leader status")
	}

	leaderHeadOffset := ls.HeadOffset

	for _, server := range ensemble {
		if server.GetIdentifier() == leader.GetIdentifier() {
			continue
		}

		err = backoff.Retry(func() error {
			return s.isFollowerCatchUp(ctx, server, leaderHeadOffset)
		}, time2.NewBackOff(ctx))

		if err != nil {
			return errors.Wrap(err, "failed to get the follower status")
		}
	}

	s.log.Info("All the followers are caught up after node-swap")
	return nil
}

func (s *shardController) SyncServerAddress() {
	s.shardMetadataMutex.RLock()
	defer s.shardMetadataMutex.RUnlock()
	needSync := false
	for _, candidate := range s.shardMetadata.Ensemble {
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

func listContains(list []model.Server, sa model.Server) bool {
	for _, item := range list {
		if item.GetIdentifier() == sa.GetIdentifier() {
			return true
		}
	}

	return false
}

func mergeLists[T any](lists ...[]T) []T {
	var res []T
	for _, list := range lists {
		res = append(res, list...)
	}
	return res
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
