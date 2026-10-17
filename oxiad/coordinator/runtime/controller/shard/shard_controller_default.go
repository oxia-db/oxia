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

package shard

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"

	featurepkg "github.com/oxia-db/oxia/oxiad/common/feature"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/leader"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"

	"github.com/oxia-db/oxia/common"
	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/constant"
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

var _ Controller = &controller{}

type controller struct {
	ctx                                 context.Context
	ctxCancel                           context.CancelFunc
	wg                                  sync.WaitGroup
	logger                              *slog.Logger
	namespace                           string
	shard                               int64
	namespaceConfig                     *proto.Namespace
	periodicTasksInterval               time.Duration
	rpc                                 rpc.Provider
	leaderSelector                      selector.Selector[*leaderselector.Context, *proto.DataServerIdentity]
	eventListener                       controllerapi.ShardEventListener
	metadataStore                       coordmetadata.Metadata
	dataServerSupportedFeaturesSupplier FeaturesSupplier

	electionOp          chan *action.ElectionAction
	deleteOp            chan any
	dataServerFailureOp chan *proto.DataServerIdentity
	changeEnsembleOp    chan *action.ChangeEnsembleAction

	// The features of a data server are reported only when they change, so
	// unlike the other notifications they must not be dropped when the event
	// loop is busy: the data servers are collected until the loop handles
	// them, and featuresDiscoveredOp wakes it up.
	discoveredDataServersMu sync.Mutex
	discoveredDataServers   map[string]*proto.DataServerIdentity
	featuresDiscoveredOp    chan any
	// termFeaturesCheckPending is set from the discovery of the features of
	// an ensemble member until checkTermFeatures completes. It is owned by
	// the event loop.
	termFeaturesCheckPending bool

	terminating   atomic.Bool
	closeOnce     sync.Once
	terminationMu sync.RWMutex

	// currentElection is owned by the event loop, the only goroutine that
	// replaces it. The term gauge, which runs on the goroutine that collects
	// the metrics, borrows it to read its term: hence the atomic pointer.
	currentElection atomic.Pointer[Election]

	leaderElectionLatencyHistogram metric.LatencyHistogram
	newTermQuorumLatencyHistogram  metric.LatencyHistogram
	becomeLeaderLatencyHistogram   metric.LatencyHistogram
	failedLeaderElectionsCounter   metric.Counter
	termGauge                      metric.Gauge
}

func (s *controller) BecameUnavailable(dataServer *proto.DataServerIdentity) {
	s.terminationMu.RLock()
	defer s.terminationMu.RUnlock()
	if s.terminating.Load() {
		return
	}
	if !channel.PushNoBlock(s.dataServerFailureOp, dataServer) {
		s.logger.Debug(
			"Discarding data server failure notification because queue is full",
			slog.Any("data-server", dataServer),
		)
	}
}

func (s *controller) FeaturesDiscovered(dataServer *proto.DataServerIdentity) {
	s.terminationMu.RLock()
	defer s.terminationMu.RUnlock()
	if s.terminating.Load() {
		return
	}
	s.discoveredDataServersMu.Lock()
	s.discoveredDataServers[dataServer.GetNameOrDefault()] = dataServer
	s.discoveredDataServersMu.Unlock()
	// A wake-up that is pending already covers this data server too
	channel.PushNoBlock(s.featuresDiscoveredOp, nil)
}

//nolint:revive
func NewController(
	namespace string,
	shard int64,
	nc *proto.Namespace,
	shardMetadata *proto.ShardMetadata,
	metadataStore coordmetadata.Metadata,
	dataServerSupportedFeaturesSupplier FeaturesSupplier,
	eventListener controllerapi.ShardEventListener,
	rpcProvider rpc.Provider,
	periodTasksInterval time.Duration) Controller {
	labels := metric.LabelsForShard(namespace, shard)
	s := &controller{
		namespace:                           namespace,
		shard:                               shard,
		namespaceConfig:                     nc,
		rpc:                                 rpcProvider,
		metadataStore:                       metadataStore,
		dataServerSupportedFeaturesSupplier: dataServerSupportedFeaturesSupplier,
		eventListener:                       eventListener,
		leaderSelector:                      leaderselector.NewSelector(),
		electionOp:                          make(chan *action.ElectionAction, chanBufferSize),
		deleteOp:                            make(chan any, chanBufferSize),
		dataServerFailureOp:                 make(chan *proto.DataServerIdentity, chanBufferSize),
		changeEnsembleOp:                    make(chan *action.ChangeEnsembleAction, chanBufferSize),
		discoveredDataServers:               make(map[string]*proto.DataServerIdentity),
		featuresDiscoveredOp:                make(chan any, 1),

		periodicTasksInterval: oxiatime.Jitter(periodTasksInterval, periodTasksInterval/4),
		logger: slog.With(
			slog.String("component", "shard-controller"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shard),
		),
		wg: sync.WaitGroup{},

		leaderElectionLatencyHistogram: metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		failedLeaderElectionsCounter: metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels),
		newTermQuorumLatencyHistogram: metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		becomeLeaderLatencyHistogram: metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
	}

	s.termGauge = metric.NewGauge("oxia_coordinator_term",
		"The term of the shard", "count", labels, func() int64 {
			if e := s.currentElection.Load(); e != nil {
				return e.Term()
			}
			return constant.I64NegativeOne
		})

	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	s.logger.Info("Started shard controller", slog.Any("shard-metadata", shardMetadata))

	s.wg.Go(func() {
		process.DoWithLabels(
			s.ctx,
			map[string]string{
				"oxia":      "shard-controller",
				"namespace": s.namespace,
				"shard":     fmt.Sprintf("%d", s.shard),
			}, func() {
				// Retry until the shard controller closes
				bo := oxiatime.NewBackOff(s.ctx)
				_ = backoff.RetryNotify(func() error {
					return s.run(bo)
				}, bo, func(err error, duration time.Duration) {
					s.logger.Warn(
						"Failed to start the shard controller, retrying later",
						slog.Any("error", err),
						slog.Duration("retry-after", duration),
					)
				})
			},
		)
	})

	return s
}

func (s *controller) Dispatch(ac action.Action) {
	s.terminationMu.RLock()
	defer s.terminationMu.RUnlock()
	if s.terminating.Load() {
		ac.Error(constant.ErrResourceUnavailable)
		return
	}
	switch ac := ac.(type) {
	case *action.ElectionAction:
		if !channel.PushNoBlock(s.electionOp, ac) {
			s.logger.Debug("Discarding leader election because queue is full")
			ac.Error(fmt.Errorf("%w: the election queue is full", constant.ErrResourceUnavailable))
		}
	case *action.ChangeEnsembleAction:
		select {
		case s.changeEnsembleOp <- ac:
		case <-s.ctx.Done():
			ac.Error(constant.ErrResourceUnavailable)
		}
	default:
		panic(fmt.Sprintf("unknown action type: %T", ac))
	}
}

func (s *controller) run(bo backoff.BackOff) error {
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		return err
	}
	initShardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while starting shard controller: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()

	if err := s.start(initShardMeta); err != nil {
		return err
	}

	s.logger.Info("Shard is ready", slog.Any("leader", initShardMeta.Leader))
	// A failure from now on is retried from the initial interval
	bo.Reset()

	periodicTasksTimer := time.NewTimer(s.periodicTasksInterval)
	defer periodicTasksTimer.Stop()

	// The event loop must not block on a failure. A handler that can't
	// complete, e.g. because the shard metadata can't be loaded, sends its
	// notification to the channel again a second later instead of retrying
	// in place. A change of the ensemble fails fast instead: its caller gets
	// the error and retries it.
	for !s.terminating.Load() {
		select {
		case <-s.ctx.Done():
			return nil
		case <-s.deleteOp:
			s.deleteShardWithRetries()
		case n := <-s.dataServerFailureOp:
			s.handleDataServerFailureWithRetries(n)
		case <-s.featuresDiscoveredOp:
			s.handleFeaturesDiscoveredWithRetries()
		case op := <-s.changeEnsembleOp:
			s.onChangeEnsemble(op)
		case <-periodicTasksTimer.C:
			s.handlePeriodicTasks()
			periodicTasksTimer.Reset(s.periodicTasksInterval)
		case electionAction := <-s.electionOp:
			s.handleElectionWithRetries(electionAction)
		}
	}
	s.logger.Info("Shard controller is stopped, stopping event loop")
	return nil
}

// start does the initial check or leader election of the shard, when the
// controller starts.
func (s *controller) start(initShardMeta *proto.ShardMetadata) error {
	switch {
	case initShardMeta.GetStatusOrDefault() == proto.ShardStatusDeleting:
		s.Delete()
	case initShardMeta.Split != nil && len(initShardMeta.Split.ChildShardIds) == 0:
		// Child shard during a split: the SplitController manages its lifecycle.
		// Wait until the split is complete (Split metadata cleared) before
		// entering the normal event loop, to prevent the load balancer from
		// triggering elections that would interfere with the split controller.
		s.logger.Info("Child shard during split, waiting for split to complete")
		s.waitForSplitComplete()
	case initShardMeta.GetRestore() != nil:
		// A shard created by a restore: its members install the backup before
		// its first election
		if err := s.restoreShard(initShardMeta); err != nil {
			return err
		}
		s.electLeader(initShardMeta, nil)
	case initShardMeta.Leader == nil || initShardMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState:
		s.electLeader(initShardMeta, nil)
	default:
		s.logger.Info(
			"There is already a data server marked as leader on the shard, verifying",
			slog.Any("current-leader", initShardMeta.Leader),
		)

		if !s.verifyCurrentEnsemble(initShardMeta) {
			s.electLeader(initShardMeta, nil)
		} else {
			s.Refresh()
		}
	}
	return nil
}

// waitForSplitComplete blocks until the Split metadata is cleared from this
// shard in the status resource, indicating the split controller has finished
// and the shard can operate normally. This prevents the load balancer from
// triggering elections that would interfere with the split controller.
func (s *controller) waitForSplitComplete() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
			if err != nil {
				s.logger.Warn("Failed to load the shard metadata, retrying later", slog.Any("error", err))
				continue
			}
			if !exists {
				continue
			}
			meta := borrowedMeta.UnsafeBorrow()
			if meta.Split == nil {
				s.logger.Info("Split complete, child shard entering normal operation",
					slog.Any("leader", meta.Leader),
				)
				return
			}
		}
	}
}

func (s *controller) handleDataServerFailureWithRetries(failedDataServer *proto.DataServerIdentity) {
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		s.logger.Warn(
			"Failed to handle the data server failure, retrying later",
			slog.Any("failed-data-server", failedDataServer.GetNameOrDefault()),
			slog.Any("error", err),
		)
		time.AfterFunc(time.Second, func() { s.BecameUnavailable(failedDataServer) })
		return
	}
	shardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while handling data server failure: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()
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
		s.electLeader(shardMeta, nil)
	}
}

// handleFeaturesDiscoveredWithRetries checks the features pinned by the current term
// when the features of ensemble members are discovered (see
// checkTermFeatures). The features of the other data servers don't change the
// ones that the ensemble supports.
func (s *controller) handleFeaturesDiscoveredWithRetries() {
	s.discoveredDataServersMu.Lock()
	discovered := s.discoveredDataServers
	s.discoveredDataServers = make(map[string]*proto.DataServerIdentity)
	s.discoveredDataServersMu.Unlock()

	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		// The features are notified once: send them again to the event loop
		// later
		s.logger.Warn("Failed to check the term features, retrying later", slog.Any("error", err))
		time.AfterFunc(time.Second, func() {
			for _, dataServer := range discovered {
				s.FeaturesDiscovered(dataServer)
			}
		})
		return
	}
	shardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while handling discovered features: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()
	if slices.ContainsFunc(shardMeta.Ensemble, func(member *proto.DataServerIdentity) bool {
		_, found := discovered[member.GetNameOrDefault()]
		return found
	}) {
		s.termFeaturesCheckPending = true
	}
	s.checkTermFeatures(shardMeta)
}

// checkTermFeatures starts a new election when the whole ensemble supports
// features that the current term doesn't pin, or the shard would run without
// them until its next election. A term pins the features of the members as
// far as they were known when it was negotiated, and they can change
// afterward: e.g. in a rolling upgrade, the leader restarts last, and the
// election that replaces it negotiates the features of its previous binary;
// or the first handshake of a member with this coordinator completes after
// the election. The periodic tasks retry the check while it can't complete,
// e.g. during a split of the shard.
func (s *controller) checkTermFeatures(shardMeta *proto.ShardMetadata) {
	if !s.termFeaturesCheckPending {
		return
	}
	if shardMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState || shardMeta.Split != nil {
		return
	}
	ensemble := shardMeta.Ensemble
	supported := negotiate(s.dataServerSupportedFeaturesSupplier(ensemble), len(ensemble))
	if len(supported) == 0 {
		// e.g. the features of a member are not known yet
		s.termFeaturesCheckPending = false
		return
	}
	pinned, err := s.termFeatures(shardMeta)
	switch {
	case errors.Is(err, ErrTermFeaturesNotReported):
		// The leader runs an older binary: the election that replaces it when
		// it restarts with a newer one negotiates the features again
		s.termFeaturesCheckPending = false
		return
	case err != nil:
		s.logger.Warn(
			"Failed to read the features pinned by the term, retrying later",
			slog.Any("leader", shardMeta.Leader),
			slog.Any("error", err),
		)
		return
	}
	s.termFeaturesCheckPending = false
	features := featurepkg.Missing(supported, pinned)
	if len(features) == 0 {
		return
	}
	s.logger.Info(
		"Starting a new election to pin the features supported by the ensemble",
		slog.Int64("term", shardMeta.Term),
		slog.Any("pinned-features", pinned),
		slog.Any("features", features),
	)
	s.electLeader(shardMeta, nil)
}

// termFeatures returns the features pinned by the current term of the shard.
// They are known if the controller elected the leader of the term; otherwise,
// e.g. after a restart of the coordinator, the leader reports them.
func (s *controller) termFeatures(shardMeta *proto.ShardMetadata) ([]proto.Feature, error) {
	if e := s.currentElection.Load(); e != nil && e.mutableShardMetadata.GetLeader() != nil &&
		e.mutableShardMetadata.GetTerm() == shardMeta.GetTerm() {
		return e.pinnedFeatures, nil
	}
	status, err := s.rpc.GetStatus(s.ctx, shardMeta.Leader, &proto.GetStatusRequest{Shard: s.shard})
	switch {
	case err != nil:
		return nil, err
	case status.GetTerm() != shardMeta.GetTerm() || status.GetStatus() != proto.ServingStatus_LEADER:
		return nil, fmt.Errorf("the leader is in term %d with status %s", status.GetTerm(), status.GetStatus())
	case status.GetTermFeatures() == nil:
		return nil, ErrTermFeaturesNotReported
	}
	return status.GetTermFeatures().GetFeatures(), nil
}

func (s *controller) verifyCurrentEnsemble(initShardMeta *proto.ShardMetadata) bool {
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

func (s *controller) validateChangeEnsembleFeatures(shardMeta *proto.ShardMetadata,
	changeEnsembleAction *action.ChangeEnsembleAction) error {
	// The ensemble of the parent and of the children of a split doesn't change
	// until the split ends. Checking it before the election that changes the
	// ensemble keeps the current election of the shard running; the election
	// checks it again, in the status update that starts it, in case a split
	// started since (see Election.persistNewTerm).
	if shardMeta.Split != nil {
		return fmt.Errorf("%w: the shard is part of a split", ErrNotReadyForChangeEnsemble)
	}
	if changeEnsembleAction.From == nil {
		return fmt.Errorf("%w: from data server is nil", ErrInvalidChangeEnsemble)
	}
	if changeEnsembleAction.To == nil {
		return fmt.Errorf("%w: to data server is nil", ErrInvalidChangeEnsemble)
	}
	source := changeEnsembleAction.From.GetNameOrDefault()
	target := changeEnsembleAction.To.GetNameOrDefault()
	ensembleSize := len(shardMeta.Ensemble)
	targetEnsemble := slices.DeleteFunc(slices.Clone(shardMeta.Ensemble), func(dataServer *proto.DataServerIdentity) bool {
		return dataServer.GetNameOrDefault() == source
	})
	if len(targetEnsemble) != ensembleSize-1 {
		return fmt.Errorf("%w: source data server %q must appear exactly once in the current ensemble", ErrInvalidChangeEnsemble, source)
	}
	ensembleWithoutTarget := slices.DeleteFunc(slices.Clone(shardMeta.Ensemble), func(dataServer *proto.DataServerIdentity) bool {
		return dataServer.GetNameOrDefault() == target
	})
	if len(ensembleWithoutTarget) != ensembleSize {
		return fmt.Errorf("%w: target data server %q is already in the current ensemble", ErrInvalidChangeEnsemble, target)
	}
	targetEnsemble = append(targetEnsemble, changeEnsembleAction.To)

	currentFeatures := s.dataServerSupportedFeaturesSupplier(shardMeta.Ensemble)
	if len(currentFeatures) != ensembleSize {
		return fmt.Errorf("%w: found feature info for %d of %d current ensemble data servers",
			ErrNotReadyForChangeEnsemble, len(currentFeatures), ensembleSize)
	}
	requiredFeatures := negotiate(currentFeatures, ensembleSize)
	targetFeatureInfo := s.dataServerSupportedFeaturesSupplier(targetEnsemble)
	if len(targetFeatureInfo) != len(targetEnsemble) {
		return fmt.Errorf("%w: found feature info for %d of %d target ensemble data servers",
			ErrNotReadyForChangeEnsemble, len(targetFeatureInfo), len(targetEnsemble))
	}
	if len(requiredFeatures) == 0 {
		return nil
	}
	targetFeatures := negotiate(targetFeatureInfo, len(targetEnsemble))
	var missing []proto.Feature
	for _, feature := range requiredFeatures {
		if !slices.Contains(targetFeatures, feature) {
			missing = append(missing, feature)
		}
	}
	if len(missing) > 0 {
		s.logger.Warn(
			"Change ensemble rejected: target ensemble does not support negotiated shard features",
			slog.Any("required-features", requiredFeatures),
			slog.Any("target-features", targetFeatures),
			slog.Any("missing-features", missing),
			slog.Any("current-ensemble", shardMeta.Ensemble),
			slog.Any("target-ensemble", targetEnsemble),
		)
		return fmt.Errorf("%w: missing features %v", ErrChangeEnsembleLosesFeatureSupport, missing)
	}
	return nil
}

func (s *controller) handleElectionWithRetries(electionAction *action.ElectionAction) {
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		s.logger.Warn("Failed to start the election, retrying later", slog.Any("error", err))
		time.AfterFunc(time.Second, func() { s.Dispatch(electionAction) })
		return
	}
	shardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while starting election: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()
	leader := s.electLeader(shardMeta, nil)
	if leader == nil {
		electionAction.Error(ErrElectionFailed)
		return
	}
	electionAction.Done(leader.GetNameOrDefault())
}

// electLeader starts a new election of the shard, with its current metadata,
// in place of the current one.
func (s *controller) electLeader(shardMeta *proto.ShardMetadata,
	changeEnsembleAction *action.ChangeEnsembleAction) *proto.DataServerIdentity {
	// stop the current term election
	if e := s.currentElection.Swap(nil); e != nil {
		e.Stop()
	}

	election := NewElection(s.ctx, s.logger, s.eventListener,
		s.metadataStore, s.dataServerSupportedFeaturesSupplier, s.leaderSelector,
		s.rpc, s.namespace, s.shard, shardMeta, changeEnsembleAction,
		s.leaderElectionLatencyHistogram,
		s.newTermQuorumLatencyHistogram,
		s.becomeLeaderLatencyHistogram,
		s.failedLeaderElectionsCounter)
	s.currentElection.Store(election)
	return election.Start()
}

func (s *controller) Delete() {
	s.terminationMu.RLock()
	defer s.terminationMu.RUnlock()
	if s.terminating.Load() {
		return
	}
	// A pending deletion covers this one too
	channel.PushNoBlock(s.deleteOp, nil)
}

func (s *controller) deleteShardWithRetries() {
	s.logger.Info("Deleting shard")
	if err := s.deleteShard(); err != nil {
		s.logger.Warn("Delete shard failed, retrying later", slog.Any("error", err))
		time.AfterFunc(time.Second, s.Delete)
	}
}

func (s *controller) deleteShard() error {
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		return err
	}
	shardMeta := gproto.CloneOf(common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while deleting shard: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow())
	for _, server := range shardMeta.Ensemble {
		// We need to save the address because it gets modified in the loop
		if _, err := s.rpc.DeleteShard(s.ctx, server, &proto.DeleteShardRequest{
			Namespace: s.namespace,
			Shard:     s.shard,
			Term:      shardMeta.Term,
		}); err != nil {
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

	s.terminationMu.Lock()
	err = s.metadataStore.DeleteShardStatus(s.namespace, s.shard)
	if err == nil {
		s.terminating.Store(true)
	}
	s.terminationMu.Unlock()
	if err != nil {
		return err
	}
	if s.eventListener != nil {
		go func() {
			process.DoWithLabels(
				context.Background(),
				map[string]string{
					"oxia":      "shard-controller-deleted-callback",
					"namespace": s.namespace,
					"shard":     fmt.Sprintf("%d", s.shard),
				}, func() {
					s.eventListener.ShardDeleted(s.shard)
				},
			)
		}()
	}
	return nil
}

func (s *controller) Close() error {
	s.closeOnce.Do(func() {
		s.ctxCancel()
		// Cancel first so any sender blocked while holding terminationMu.RLock can
		// release it before Close waits for the enqueue barrier.
		s.terminationMu.Lock()
		s.terminating.Store(true)
		s.terminationMu.Unlock()

		// NOTE: we must wait the run goroutine to exit, otherwise
		// the controller maybe running after close is returned.
		s.wg.Wait()

	drainPendingOps:
		for {
			select {
			case electionAction := <-s.electionOp:
				electionAction.Error(constant.ErrResourceUnavailable)
			case <-s.deleteOp:
			case <-s.dataServerFailureOp:
			case <-s.featuresDiscoveredOp:
			case op := <-s.changeEnsembleOp:
				op.Error(constant.ErrResourceUnavailable)
			default:
				break drainPendingOps
			}
		}

		s.termGauge.Unregister()
	})
	return nil
}

func (s *controller) onChangeEnsemble(changeEnsembleAction *action.ChangeEnsembleAction) {
	if e := s.currentElection.Load(); e != nil {
		if ready := e.IsReadyForChangeEnsemble(); !ready {
			s.logger.Warn("Change ensemble rejected: shard is not ready (follower catch-up still in progress)")
			changeEnsembleAction.Error(ErrNotReadyForChangeEnsemble)
			return
		}
	}
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		changeEnsembleAction.Error(err)
		return
	}
	shardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while changing the ensemble: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()
	if err := s.validateChangeEnsembleFeatures(shardMeta, changeEnsembleAction); err != nil {
		changeEnsembleAction.Error(err)
		return
	}
	// todo: support optimized ensemble change to avoid start a new election
	leader := s.electLeader(shardMeta, changeEnsembleAction)
	election := s.currentElection.Load()
	if err := election.ChangeEnsembleError(); err != nil {
		changeEnsembleAction.Error(err)
		return
	}
	if leader == nil {
		// The election stopped without changing the ensemble, e.g. because a
		// split of the shard started after the validation above. The shard is
		// as ready for a change of its ensemble as it was before this one: drop
		// the election, which never elected a leader for its followers to catch
		// up with, or it rejects the next changes as not ready.
		election.Stop()
		s.currentElection.Store(nil)
		changeEnsembleAction.Error(fmt.Errorf("%w: the election stopped before changing the ensemble",
			ErrNotReadyForChangeEnsemble))
		return
	}
	changeEnsembleAction.Done(nil)
}

func (s *controller) Refresh() {
	s.terminationMu.RLock()
	defer s.terminationMu.RUnlock()
	if s.terminating.Load() {
		return
	}
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		s.logger.Warn("Failed to refresh the ensemble addresses", slog.Any("error", err))
		return
	}
	if !exists && s.terminating.Load() {
		// The shard was deleted since the check above
		return
	}
	shardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while syncing server addresses: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()
	needSync := false
	for _, candidate := range shardMeta.Ensemble {
		borrowedDataServer, ok, err := s.metadataStore.GetDataServer(candidate.GetNameOrDefault())
		if err != nil {
			s.logger.Warn("Failed to refresh the ensemble addresses", slog.Any("error", err))
			return
		}
		if ok {
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
	if !channel.PushNoBlock(s.electionOp, action.NewElectionAction(s.shard)) {
		s.logger.Debug("Discarding refresh election because queue is full")
	}
}

func (s *controller) handlePeriodicTasks() {
	borrowedMeta, exists, err := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if err != nil {
		s.logger.Warn("Failed to handle the periodic tasks, retrying next round", slog.Any("error", err))
		return
	}
	shardMeta := common.Must(borrowedMeta, exists,
		"bug: shard metadata missing while handling periodic tasks: namespace=", s.namespace, " shard=",
		s.shard).UnsafeBorrow()

	if len(shardMeta.PendingDeleteShardNodes) > 0 {
		if err := s.handlePendingDeleteShard(shardMeta); err != nil {
			s.logger.Warn("Failed to handle pending delete shard", slog.Any("error", err))
		}
	}

	s.checkTermFeatures(shardMeta)
}

// handlePendingDeleteShard deletes the shard from the data servers removed from
// its ensemble, then drops them from the pending-delete nodes, on top of the
// current metadata of the shard. Other writers update the shard concurrently,
// e.g. a namespace deletion marks it Deleting, and a write of the copy read
// before the deletions would revert them.
func (s *controller) handlePendingDeleteShard(shardMeta *proto.ShardMetadata) error {
	deleted := make(map[string]bool, len(shardMeta.PendingDeleteShardNodes))
	for _, ds := range shardMeta.PendingDeleteShardNodes {
		s.logger.Info("Deleting shard from removed data server", slog.Any("data-server", ds))

		if _, err := s.rpc.DeleteShard(s.ctx, ds, &proto.DeleteShardRequest{
			Namespace: s.namespace,
			Shard:     s.shard,
			Term:      shardMeta.Term,
		}); err != nil {
			return fmt.Errorf("delete shard from removed data server %s: %w", ds.GetNameOrDefault(), err)
		}

		s.logger.Info("Successfully deleted shard from data server", slog.Any("data-server", ds))
		deleted[ds.GetNameOrDefault()] = true
	}

	// Best-effort: if this is not persisted, the pending deletes are retried
	_ = s.metadataStore.PatchNamespaceStatus(s.namespace, func(status *proto.NamespaceStatus) bool {
		shards := status.Shards
		current, exists := shards[s.shard]
		if !exists {
			return false
		}
		pending := len(current.PendingDeleteShardNodes)
		current.PendingDeleteShardNodes = slices.DeleteFunc(current.PendingDeleteShardNodes,
			func(ds *proto.DataServerIdentity) bool { return deleted[ds.GetNameOrDefault()] })
		return len(current.PendingDeleteShardNodes) < pending
	})
	return nil
}
