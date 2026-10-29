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

package election

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/entry"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/common/quorum"
	"github.com/oxia-db/oxia/oxiad/common/sharding"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/leader"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/controller"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commonrpc "github.com/oxia-db/oxia/common/rpc"
	oxiatime "github.com/oxia-db/oxia/common/time"
)

// When fencing quorum of servers, after we reach the majority, wait a bit more
// to include responses from all healthy servers.
const quorumFencingGracePeriod = 100 * time.Millisecond

var _ Election = &election{}

type election struct {
	logger           *slog.Logger
	ctx              context.Context
	ctxCancel        context.CancelFunc
	wg               sync.WaitGroup
	metadataStore    coordmetadata.Metadata
	leaderSelector   selector.Selector[*leaderselector.Context, *proto.DataServerIdentity]
	eventListener    controllerapi.ShardEventListener
	provider         rpc.ControllerRPCProvider
	namespace        string
	shard            int64
	changeEnsemble   *action.ChangeEnsembleAction
	requiredFeatures []proto.Feature
	featureSupplier  feature.Supplier

	// mutable
	meta             *proto.ShardMetadata
	started          atomic.Bool
	followerCaughtUp atomic.Bool

	// metrics
	leaderElectionLatencyHistogram metric.LatencyHistogram
	newTermQuorumLatencyHistogram  metric.LatencyHistogram
	becomeLeaderLatencyHistogram   metric.LatencyHistogram
}

func (e *election) refreshEnsembleInfo(ensemble []*proto.DataServerIdentity) ([]*proto.DataServerIdentity, error) {
	refreshedEnsembleDataServerAddress := make([]*proto.DataServerIdentity, len(ensemble))
	for idx, candidate := range ensemble {
		borrowedDataServer, exist, err := e.metadataStore.GetDataServer(candidate.GetNameOrDefault())
		if err != nil {
			return nil, err
		}
		if exist {
			if identity := borrowedDataServer.UnsafeBorrow().GetIdentity(); identity != nil {
				refreshedEnsembleDataServerAddress[idx] = identity
				continue
			}
		}
		refreshedEnsembleDataServerAddress[idx] = candidate
	}
	if e.logger.Enabled(e.ctx, slog.LevelDebug) {
		if !reflect.DeepEqual(ensemble, refreshedEnsembleDataServerAddress) {
			e.logger.Info("refresh the shard ensemble dataServer address", slog.Any("current-ensemble", ensemble),
				slog.Any("new-ensemble", refreshedEnsembleDataServerAddress))
		}
	}
	return refreshedEnsembleDataServerAddress, nil
}

//nolint:revive
func (e *election) fenceQuorum(term int64, options *proto.NewTermOptions, ensemble []*proto.DataServerIdentity,
	retained []*proto.DataServerIdentity) (ensembleHeads sharding.HeadEntries,
	retainedHeads sharding.HeadEntries, features []proto.Feature, err error) {
	fenceQuorumTimer := e.newTermQuorumLatencyHistogram.Timer()

	fencingDataServers := slices.Concat(ensemble, retained)
	fencingQuorumSize := len(fencingDataServers)
	// todo: require a majority of the ensemble before the change and of the one
	//  after instead, which is enough and tolerates a failure of the new member
	//  while a dead retained data server isn't released; it needs the ensemble
	//  before the change in the shard metadata
	majority := quorum.Majority(fencingQuorumSize)

	// Use a new context, so we can cancel the pending requests
	fencingContext, fencingContextCancel := context.WithCancel(e.ctx)
	latch := sync.WaitGroup{}
	defer func() {
		fencingContextCancel()
		latch.Wait()
		fenceQuorumTimer.Done()
	}()

	type fenceResponse struct {
		DataServer *proto.DataServerIdentity
		Response   *proto.NewTermResponse
		Err        error
	}
	// Channel to receive responses or errors from each server
	ch := make(chan fenceResponse, fencingQuorumSize)

	for _, server := range fencingDataServers {
		latch.Go(func() {
			process.DoWithLabels(
				fencingContext,
				map[string]string{
					"oxia":        "election-fence-new-term",
					"shard":       fmt.Sprintf("%d", e.shard),
					"data-server": server.GetNameOrDefault(),
				}, func() {
					res, err := e.provider.NewTerm(fencingContext, server, &proto.NewTermRequest{
						Namespace: e.namespace,
						Shard:     e.shard,
						Term:      term,
						Options:   options,
					})
					switch {
					case errors.Is(err, constant.ErrNotInitialized):
						e.logger.Debug("FenceNewTerm is waiting for data server initialization", slog.Any("data-server", server))
					case err != nil:
						e.logger.Warn("FenceNewTerm failed", slog.Any("error", err), slog.Any("data-server", server))
					default:
						e.logger.Info("Processed fenceNewTerm response", slog.Any("data-server", server), slog.Any("entry-id", res.HeadEntryId))
					}
					ch <- fenceResponse{DataServer: server, Response: res, Err: err}
				},
			)
		})
	}
	enabledFeatures := make(map[proto.Feature]bool)
	retainedHeads = make(sharding.HeadEntries)
	ensembleHeads = make(sharding.HeadEntries)
	successResponses := 0
	totalResponses := 0
	// The new leader also needs a majority of the new ensemble as followers
	ensembleMajority := quorum.Majority(len(ensemble))
	for (successResponses < majority || len(ensembleHeads) < ensembleMajority) &&
		totalResponses < fencingQuorumSize {
		fencingResponse := <-ch

		totalResponses++
		if fencingResponse.Err != nil {
			err = multierr.Append(err, fencingResponse.Err)
			continue
		}
		successResponses++
		for _, f := range fencingResponse.Response.GetFeaturesEnabled() {
			enabledFeatures[f] = true
		}
		if slices.Contains(ensemble, fencingResponse.DataServer) {
			ensembleHeads[fencingResponse.DataServer] = fencingResponse.Response.HeadEntryId
		} else {
			retainedHeads[fencingResponse.DataServer] = fencingResponse.Response.HeadEntryId
		}
	}
	if successResponses < majority {
		return nil, nil, nil, errors.Wrap(err, "election failed: quorum not reached")
	}
	if len(ensembleHeads) < ensembleMajority {
		return nil, nil, nil, errors.Errorf(
			"election failed: quorum of the new ensemble not reached: %d of %d members fenced",
			len(ensembleHeads), len(ensemble))
	}
	// If we have already reached a quorum of successful responses, we can wait a
	// tiny bit more, to allow time for all the "healthy" data servers to respond.
gracePeriod:
	for totalResponses < fencingQuorumSize {
		select {
		case r := <-ch:
			totalResponses++
			if r.Err != nil {
				// rpc has already printed the logs
				continue
			}
			for _, f := range r.Response.GetFeaturesEnabled() {
				enabledFeatures[f] = true
			}
			if slices.Contains(ensemble, r.DataServer) {
				ensembleHeads[r.DataServer] = r.Response.HeadEntryId
			} else {
				retainedHeads[r.DataServer] = r.Response.HeadEntryId
			}
		case <-time.After(quorumFencingGracePeriod):
			break gracePeriod
		}
	}
	enabled := maps.Keys(enabledFeatures)
	slices.Sort(enabled)
	return ensembleHeads, retainedHeads, enabled, nil
}

// chooseLeader chooses a fenced data server with the highest head entry, which
// has every committed entry:
//
//	           +----------------------+
//	heads ---->| 1. ensemble          |    the highest head entry, with its ties
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 2. retained          |--> one strictly ahead: it swaps with the
//	           |                      |    lowest member of the ensemble, and the
//	           |                      |    choice starts again
//	           +----------------------+
//	                      | none
//	           +----------------------+
//	           | 3. select            |    the leader among the ties, the others
//	           |                      |    follow
//	           +----------------------+
func (e *election) chooseLeader(ensembleHeads sharding.HeadEntries, retainedHeads sharding.HeadEntries) (
	leader *proto.DataServerIdentity, followers sharding.HeadEntries, err error) {
	var candidates []*proto.DataServerIdentity
	var highest *proto.EntryId
	for i, heads := range []sharding.HeadEntries{ensembleHeads, retainedHeads} {
		for server, head := range heads {
			if highest == nil || entry.Compare(head, highest) > 0 {
				highest, candidates = head, []*proto.DataServerIdentity{server}
			} else if i == 0 && entry.Compare(head, highest) == 0 {
				// Visited first, the ensemble wins a tie
				candidates = append(candidates, server)
			}
		}
	}
	if len(candidates) > 0 {
		if _, retained := retainedHeads[candidates[0]]; retained {
			replaced := slices.MinFunc(maps.Keys(ensembleHeads), func(a, b *proto.DataServerIdentity) int {
				return entry.Compare(ensembleHeads[a], ensembleHeads[b])
			})
			e.logger.Warn("A retained data server is ahead of the ensemble, it goes back into the ensemble",
				slog.Any("restored", candidates[0]), slog.Any("replaced", replaced))
			ensembleHeads.Move(replaced, retainedHeads)
			retainedHeads.Move(candidates[0], ensembleHeads)
			sharding.MoveDataServer(replaced, &e.meta.Ensemble, &e.meta.RemovedNodes)
			sharding.MoveDataServer(candidates[0], &e.meta.RemovedNodes, &e.meta.Ensemble)
			return e.chooseLeader(ensembleHeads, retainedHeads)
		}
	}
	namespaces, err := e.metadataStore.ListNamespaceStatus()
	if err != nil {
		return nil, nil, err
	}
	if leader, err = e.leaderSelector.Select(&leaderselector.Context{
		Candidates: candidates,
		Namespaces: namespaces,
	}); err != nil {
		return nil, nil, err
	}
	followers = make(sharding.HeadEntries)
	for server, head := range ensembleHeads {
		if server.GetNameOrDefault() != leader.GetNameOrDefault() {
			followers[server] = head
		}
	}
	return leader, followers, nil
}

func (e *election) becomeLeader(term int64, leader *proto.DataServerIdentity, followers sharding.HeadEntries,
	replicationFactor uint32, negotiateFeatures []proto.Feature) error {
	becomeLeaderTimer := e.becomeLeaderLatencyHistogram.Timer()

	followersMap := make(map[string]*proto.EntryId)
	for server, e := range followers {
		followersMap[server.GetInternal()] = e
	}
	ctx, cancel := context.WithTimeout(e.ctx, commonrpc.DefaultRpcTimeout)
	defer cancel()
	if _, err := e.provider.BecomeLeader(ctx, leader, &proto.BecomeLeaderRequest{
		Namespace:         e.namespace,
		Shard:             e.shard,
		Term:              term,
		ReplicationFactor: replicationFactor,
		FollowerMaps:      followersMap,
		FeaturesSupported: negotiateFeatures,
	}); err != nil {
		return err
	}

	becomeLeaderTimer.Done()
	return nil
}

//nolint:revive
func (e *election) ensureFollowerCaught(ensemble []*proto.DataServerIdentity, leader *proto.DataServerIdentity,
	leaderEntry *proto.EntryId) error {
	failures := make([]error, len(ensemble))
	waitGroup := sync.WaitGroup{}
	for i, server := range ensemble {
		if server.GetNameOrDefault() == leader.GetNameOrDefault() {
			continue
		}
		waitGroup.Go(func() {
			process.DoWithLabels(
				e.ctx,
				map[string]string{
					"oxia":        "shard-caught-up-monitor",
					"namespace":   e.namespace,
					"shard":       fmt.Sprintf("%d", e.shard),
					"data-server": server.GetNameOrDefault(),
				}, func() {
					failures[i] = backoff.RetryNotify(func() error {
						fs, err := e.provider.GetStatus(e.ctx, server, &proto.GetStatusRequest{Shard: e.shard})
						if err != nil {
							return err
						}
						// Unlike its head entry, which can be of a diverging log until
						// the leader truncates it, the committed entries of a follower
						// are the ones of the leader
						if fs.CommitOffset >= leaderEntry.Offset {
							e.logger.Info(
								"Follower is caught-up with the leader after election",
								slog.Any("server", server),
							)
							return nil
						}
						e.logger.Info(
							"Follower is *not* caught-up yet with the leader",
							slog.Any("server", server),
							slog.Int64("leader-head-offset", leaderEntry.Offset),
							slog.Int64("follower-commit-offset", fs.CommitOffset),
						)
						return errors.Wrap(errs.ErrFailedPrecondition, "follower not caught up yet")
					}, oxiatime.NewBackOff(e.ctx), func(err error, duration time.Duration) {
						switch {
						case errors.Is(err, errs.ErrFailedPrecondition):
						case errors.Is(err, constant.ErrNodeIsNotMember):
							e.logger.Info("Follower has not been added by leader yet",
								slog.Any("server", server),
								slog.Int64("shard", e.shard),
								slog.Duration("retry-after", duration),
							)
						default:
							e.logger.Warn("Failed to get the follower status",
								slog.Any("server", server),
								slog.Any("error", err),
								slog.Duration("retry-after", duration),
							)
						}
					})
				})
		})
	}
	waitGroup.Wait()
	return multierr.Combine(failures...)
}

func (e *election) fenceNewTermAndAddFollower(ctx context.Context, term int64, options *proto.NewTermOptions,
	leader *proto.DataServerIdentity, follower *proto.DataServerIdentity) error {
	fr, err := e.provider.NewTerm(ctx, follower, &proto.NewTermRequest{
		Namespace: e.namespace,
		Shard:     e.shard,
		Term:      term,
		Options:   options,
	})
	if err != nil {
		return err
	}

	// Report the follower's supported features (from the latest handshake) so
	// the leader can refuse a joiner whose binary does not cover the features
	// required by the shard.
	followerFeatures := e.featureSupplier([]*proto.DataServerIdentity{follower})[follower.GetNameOrDefault()]

	if _, err := e.provider.AddFollower(ctx, leader, &proto.AddFollowerRequest{
		Namespace:    e.namespace,
		Shard:        e.shard,
		Term:         term,
		FollowerName: follower.GetInternal(),
		FollowerHeadEntryId: &proto.EntryId{
			Term:   fr.HeadEntryId.Term,
			Offset: fr.HeadEntryId.Offset,
		},
		FollowerFeatures: &proto.FollowerFeatures{Supported: followerFeatures},
	}); err != nil {
		return err
	}

	e.logger.Info(
		"Successfully rejoined the quorum",
		slog.Any("follower", follower),
		slog.Int64("term", fr.HeadEntryId.Term),
	)
	return nil
}

func (e *election) fencingFailedFollowers(term int64, options *proto.NewTermOptions, ensemble []*proto.DataServerIdentity,
	leader *proto.DataServerIdentity, successfulFollowers sharding.HeadEntries) {
	if len(successfulFollowers) == len(ensemble)-1 {
		e.logger.Debug(
			"All the member of the ensemble were successfully added",
			slog.Int64("term", term),
		)
		return
	}
	// Identify failed followers
	group := sync.WaitGroup{}
	defer group.Wait()
	for _, follower := range ensemble {
		if follower.GetNameOrDefault() == leader.GetNameOrDefault() {
			continue
		}
		if _, found := successfulFollowers[follower]; found {
			continue
		}
		e.logger.Info("Data server has failed in leader election, retrying", slog.Any("follower", follower))
		group.Go(func() {
			process.DoWithLabels(
				e.ctx,
				map[string]string{
					"oxia":     "election-retry-failed-follower",
					"shard":    fmt.Sprintf("%d", e.shard),
					"follower": follower.GetNameOrDefault(),
				},
				func() {
					bo := oxiatime.NewBackOffWithInitialInterval(e.ctx, 1*time.Second)
					_ = backoff.RetryNotify(func() error {
						err := e.fenceNewTermAndAddFollower(e.ctx, term, options, leader, follower)
						if errors.Is(err, constant.ErrInvalidTerm) {
							// If we're receiving invalid term error, it would mean
							// there's already a new term generated, and we don't have
							// to keep trying with this old term
							e.logger.Warn(
								"Failed to fenceNewTermAndAddFollower, invalid term. Stop trying",
								slog.Any("follower", follower),
								slog.Int64("term", term),
							)
							return nil
						}
						return err
					}, bo, func(err error, duration time.Duration) {
						e.logger.Warn(
							"Failed to fenceNewTermAndAddFollower, retrying later",
							slog.Any("error", err),
							slog.Any("follower", follower),
							slog.Int64("term", term),
							slog.Duration("retry-after", duration),
						)
					})
				},
			)
		})
	}
}

// prepareIfChangeEnsemble applies the change of the ensemble, unless an earlier
// election persisted it already.
func (e *election) prepareIfChangeEnsemble(mutShardMeta *proto.ShardMetadata) {
	if e.changeEnsemble == nil {
		return
	}
	from := e.changeEnsemble.From
	to := e.changeEnsemble.To
	if slices.ContainsFunc(mutShardMeta.Ensemble, func(dataServer *proto.DataServerIdentity) bool {
		return dataServer.GetNameOrDefault() == to.GetNameOrDefault()
	}) {
		return
	}
	if !slices.ContainsFunc(mutShardMeta.RemovedNodes, func(server *proto.DataServerIdentity) bool {
		return server.GetNameOrDefault() == from.GetNameOrDefault()
	}) {
		mutShardMeta.RemovedNodes = append(mutShardMeta.RemovedNodes, from)
	}
	// A dataServer might get re-added to the ensemble after it was swapped out and be in
	// pending delete state. We don't want a background task to attempt deletion anymore
	mutShardMeta.PendingDeleteShardNodes = slices.DeleteFunc(mutShardMeta.PendingDeleteShardNodes, func(dataServer *proto.DataServerIdentity) bool {
		return dataServer.GetNameOrDefault() == to.GetNameOrDefault()
	})
	mutShardMeta.Ensemble = slices.DeleteFunc(mutShardMeta.Ensemble, func(dataServer *proto.DataServerIdentity) bool {
		return dataServer.GetNameOrDefault() == from.GetNameOrDefault()
	})
	mutShardMeta.Ensemble = append(mutShardMeta.Ensemble, to)
	e.logger.Info(
		"Changing ensemble",
		slog.Any("removed-data-servers", mutShardMeta.RemovedNodes),
		slog.Any("new-ensemble", mutShardMeta.Ensemble),
		slog.Any("from", from),
		slog.Any("to", to),
	)
}

func (e *election) newTerm() (*proto.NewTermOptions, error) {
	termOptions, err := e.metadataStore.GetNamespaceTermOptions(e.namespace)
	if err != nil {
		return nil, err
	}
	refreshedEnsemble, err := e.refreshEnsembleInfo(e.meta.Ensemble)
	if err != nil {
		return nil, err
	}
	// The retained data servers are fenced too, until they're released
	refreshedRetained, err := e.refreshEnsembleInfo(e.meta.RemovedNodes)
	if err != nil {
		return nil, err
	}

	e.meta.Status = proto.ShardStatusElection
	e.meta.Leader = nil
	e.meta.Term++
	e.meta.Ensemble = refreshedEnsemble
	e.meta.RemovedNodes = refreshedRetained
	if err = e.metadataStore.PatchShardStatus(e.namespace, e.shard, func(current *proto.ShardMetadata) (bool, error) {
		switch {
		case current.GetStatusOrDefault() == proto.ShardStatusDeleting:
			return false, fmt.Errorf("%w: the shard is being deleted", errs.ErrShardNotElectable)
		case len(current.GetSplit().GetChildShardIds()) > 0 &&
			current.GetSplit().GetPhaseOrDefault() == proto.SplitPhaseFinalize:
			return false, fmt.Errorf("%w: the shard is the parent of a split past the point of no return",
				errs.ErrShardNotElectable)
		case e.changeEnsemble != nil && current.Split != nil:
			return false, fmt.Errorf("%w: the shard is part of a split", errs.ErrShardNotElectable)
		}
		current.Status = e.meta.Status
		current.Term = e.meta.Term
		current.Leader = e.meta.Leader
		current.Ensemble = e.meta.Ensemble
		current.RemovedNodes = e.meta.RemovedNodes
		current.PendingDeleteShardNodes = e.meta.PendingDeleteShardNodes
		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to persist the new term")
	}
	return termOptions, nil
}

func (e *election) Start() (candidateLeader *proto.DataServerIdentity, err error) {
	if !e.started.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("%w: the election has started already", errs.ErrFailedPrecondition)
	}
	e.logger.Info("Starting a new election")
	timer := e.leaderElectionLatencyHistogram.Timer()

	termOptions, err := e.newTerm()
	if err != nil {
		return nil, err
	}

	e.prepareIfChangeEnsemble(e.meta)

	// Negotiate the feature set across the new ensemble before fencing, so it
	// can be pinned in the term options: every member persists it with the
	// term and rejects the fence if its binary does not support it. The
	// features that an earlier attempt found already enabled on the shard are
	// pinned as well (see checkNegotiatedFeatures).
	features := e.featureSupplier(e.meta.Ensemble)
	negotiatedFeatures := feature.Union(feature.Negotiate(features, len(e.meta.Ensemble)), e.requiredFeatures)
	termOptions.Features = negotiatedFeatures

	// Send NewTerm to all the ensemble members
	ensembleHeads, retainedHeads, enabledFeatures, err := e.fenceQuorum(
		e.meta.Term,
		termOptions,
		e.meta.Ensemble,
		e.meta.RemovedNodes)
	if err != nil {
		return nil, err
	}
	if err = e.checkNegotiatedFeatures(negotiatedFeatures, enabledFeatures, ensembleHeads); err != nil {
		return nil, err
	}
	candidateLeader, followers, err := e.chooseLeader(ensembleHeads, retainedHeads)
	if err != nil {
		return nil, err
	}
	e.logger.Info(
		"Successfully moved ensemble to a new term",
		slog.Int64("term", e.meta.Term),
		slog.Any("new-leader", candidateLeader),
		slog.Any("followers", followers),
	)
	leaderHead := ensembleHeads[candidateLeader]
	// Persisted first: if BecomeLeader fails, the next election keeps it
	e.meta.Status = proto.ShardStatusSteadyState
	e.meta.Leader = candidateLeader

	if err = e.metadataStore.PatchShardStatus(e.namespace, e.shard, func(current *proto.ShardMetadata) (bool, error) {
		if current.GetStatusOrDefault() != proto.ShardStatusDeleting {
			current.Status = e.meta.Status
		}
		current.Term = e.meta.Term
		current.Leader = e.meta.Leader
		current.Ensemble = e.meta.Ensemble
		current.RemovedNodes = e.meta.RemovedNodes
		current.PendingDeleteShardNodes = e.meta.PendingDeleteShardNodes
		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to persist the new leader")
	}
	if err = e.becomeLeader(e.meta.Term, candidateLeader, followers, uint32(len(e.meta.Ensemble)),
		negotiatedFeatures); err != nil {
		return nil, err
	}
	if e.eventListener != nil {
		e.eventListener.LeaderElected(e.shard, candidateLeader, maps.Keys(followers))
	}

	e.logger.Info(
		"Elected new leader",
		slog.Int64("term", e.meta.Term),
		slog.Any("leader", e.meta.Leader),
		slog.Any("ensemble", e.meta.Ensemble),
	)
	timer.Done()

	e.wg.Go(func() {
		process.DoWithLabels(
			e.ctx,
			map[string]string{
				"oxia":  "election-fencing-failed-followers",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				e.fencingFailedFollowers(e.meta.Term, termOptions, e.meta.Ensemble, candidateLeader, followers)
			},
		)
	})
	e.wg.Go(func() {
		process.DoWithLabels(
			e.ctx,
			map[string]string{
				"oxia":  "election-monitor-followers-caught-up",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				if err := e.ensureFollowerCaught(e.meta.Ensemble, candidateLeader, leaderHead); err != nil {
					e.logger.Info("Stopped waiting for the followers to catch up", slog.Any("error", err))
					return
				}
				e.followerCaughtUp.Store(true)
			},
		)
	})
	return candidateLeader, nil
}

// checkNegotiatedFeatures validates the feature set pinned by the new term
// once the ensemble is fenced. The members' features are read again: they are
// only known after the member's handshake with this coordinator, which can
// complete while an election is running (e.g. at coordinator startup), and
// until then the member counts as not supporting any feature.
//
// todo: move the check to the feature package, and share it with the split
// controller, which makes the same check for the children of a split.
func (e *election) checkNegotiatedFeatures(negotiated []proto.Feature, enabled []proto.Feature,
	candidatesStatus sharding.HeadEntries) error {
	ensemble := e.meta.Ensemble
	features := e.featureSupplier(ensemble)

	// The features pinned by the term, and the ones already enabled on the
	// shard, must be supported by every member that takes part in the term,
	// or the unsupported members would apply entries with different semantics
	// and silently diverge. Fail the election instead: the shard stays
	// unavailable until the offending nodes are replaced.
	participantsFeatures := make(map[string][]proto.Feature, len(candidatesStatus))
	for dataServer := range candidatesStatus {
		participantsFeatures[dataServer.GetNameOrDefault()] = features[dataServer.GetNameOrDefault()]
	}
	required := feature.Union(negotiated, enabled)
	if missing := feature.Missing(required, feature.Negotiate(participantsFeatures, len(participantsFeatures))); len(missing) > 0 {
		e.logger.Error(
			"Election aborted: the ensemble does not support features already enabled on the shard",
			slog.Int64("term", e.meta.Term),
			slog.Any("missing-features", missing),
			slog.Any("negotiated-features", negotiated),
			slog.Any("data-server-features", participantsFeatures),
		)
		return errors.Wrapf(constant.ErrUnsupportedFeatures,
			"ensemble does not support features %v already enabled on shard %d", missing, e.shard)
	}

	// The members that did not take part, e.g. because they are down, are
	// checked when they join the term (see fenceNewTermAndAddFollower), and a
	// feature enabled on the shard can never be disabled again: pin the
	// enabled features even if those members' support is unknown, instead of
	// failing every election until they are back.
	if missing := feature.Missing(enabled, negotiated); len(missing) > 0 {
		return &feature.EnabledError{Features: enabled}
	}

	// A handshake completed while fencing the ensemble: pin the features that
	// the whole ensemble supports now, or they would stay disabled until the
	// next election.
	if added := feature.Missing(feature.Negotiate(features, len(ensemble)), negotiated); len(added) > 0 {
		return fmt.Errorf("%w: the ensemble now supports features %v", errs.ErrShardFeaturesChanged, added)
	}
	return nil
}

func (e *election) IsFollowerCaughtUp() bool {
	return e.followerCaughtUp.Load()
}

func (e *election) Cancel() {
	e.ctxCancel()
	e.wg.Wait()
	e.logger.Info("stopped the election", slog.Any("term", e.meta.GetTerm()))
}

// New returns an election of the shard. requiredFeatures are pinned in its
// term (see feature.EnabledError).
//
//nolint:revive
func New(ctx context.Context, logger *slog.Logger, eventListener controllerapi.ShardEventListener,
	metadataStore coordmetadata.Metadata,
	featureSupplier feature.Supplier,
	leaderSelector selector.Selector[*leaderselector.Context, *proto.DataServerIdentity],
	provider rpc.ControllerRPCProvider, namespace string, shard int64,
	shardMetadata *proto.ShardMetadata,
	changeEnsembleAction *action.ChangeEnsembleAction, requiredFeatures []proto.Feature,
	leaderElectionLatencyHistogram metric.LatencyHistogram, newTermQuorumLatencyHistogram metric.LatencyHistogram,
	becomeLeaderLatencyHistogram metric.LatencyHistogram) Election {
	current, cancelFunc := context.WithCancel(ctx)
	return &election{
		logger:                         logger,
		ctx:                            current,
		ctxCancel:                      cancelFunc,
		wg:                             sync.WaitGroup{},
		metadataStore:                  metadataStore,
		featureSupplier:                featureSupplier,
		eventListener:                  eventListener,
		leaderSelector:                 leaderSelector,
		provider:                       provider,
		namespace:                      namespace,
		shard:                          shard,
		meta:                           gproto.CloneOf(shardMetadata),
		changeEnsemble:                 changeEnsembleAction,
		requiredFeatures:               requiredFeatures,
		leaderElectionLatencyHistogram: leaderElectionLatencyHistogram,
		newTermQuorumLatencyHistogram:  newTermQuorumLatencyHistogram,
		becomeLeaderLatencyHistogram:   becomeLeaderLatencyHistogram,
	}
}
