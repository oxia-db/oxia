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
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/status"

	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/leader"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
)

var (
	ErrNotReadyForChangeEnsemble = errors.New("shard is not ready for change ensemble, please retry later")
	ErrFollowerNotCaughtUp       = errors.New("follower not caught up yet")
)

type ShardElection struct {
	*slog.Logger
	sync.WaitGroup
	context.Context
	context.CancelFunc
	// metrics
	leaderElectionLatency metric.LatencyHistogram
	newTermQuorumLatency  metric.LatencyHistogram
	becomeLeaderLatency   metric.LatencyHistogram
	leaderElectionsFailed metric.Counter
	// borrowed resource
	metadataStore                       coordmetadata.Metadata
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier
	leaderSelector                      selector.Selector[*leaderselector.Context, *proto.DataServerIdentity]
	eventListener                       ShardEventListener
	provider                            rpc.Provider
	meta                                *Metadata
	// owned status
	namespace            string
	shard                int64
	changeEnsembleAction *action.ChangeEnsembleAction
	followerCaughtUp     atomic.Bool // If the followers caught up leader after election
	termOptions          *proto.NewTermOptions
	// started
	started atomic.Bool
}

func (e *ShardElection) refreshedEnsemble(ensemble []*proto.DataServerIdentity) []*proto.DataServerIdentity {
	refreshedEnsembleDataServerAddress := make([]*proto.DataServerIdentity, len(ensemble))
	for idx, candidate := range ensemble {
		if refreshedAddress, exist := e.metadataStore.Node(candidate.GetNameOrDefault()); exist {
			refreshedEnsembleDataServerAddress[idx] = refreshedAddress
			continue
		}
		refreshedEnsembleDataServerAddress[idx] = candidate
	}
	if e.Enabled(e.Context, slog.LevelDebug) {
		if !reflect.DeepEqual(ensemble, refreshedEnsembleDataServerAddress) {
			e.Info("refresh the shard ensemble dataServer address", slog.Any("current-ensemble", ensemble),
				slog.Any("new-ensemble", refreshedEnsembleDataServerAddress))
		}
	}
	return refreshedEnsembleDataServerAddress
}

func (e *ShardElection) fenceNewTerm(ctx context.Context, term int64, dataServer *proto.DataServerIdentity) (*proto.EntryId, error) {
	res, err := e.provider.NewTerm(ctx, dataServer, &proto.NewTermRequest{
		Namespace: e.namespace,
		Shard:     e.shard,
		Term:      term,
		Options:   e.termOptions,
	})
	if err != nil {
		return nil, err
	}

	return res.HeadEntryId, nil
}

// Send NewTerm to all the ensemble members in parallel and wait for
// a majority of them to reply successfully.
func (e *ShardElection) fenceNewTermQuorum(term int64, ensemble []*proto.DataServerIdentity, removedCandidates []*proto.DataServerIdentity) (map[*proto.DataServerIdentity]*proto.EntryId, error) {
	fenceQuorumTimer := e.newTermQuorumLatency.Timer()

	fencingDataServers := slices.Concat(ensemble, removedCandidates)
	fencingQuorumSize := len(fencingDataServers)
	majority := fencingQuorumSize/2 + 1

	// Use a new context, so we can cancel the pending requests
	fencingContext, fencingContextCancel := context.WithCancel(e.Context)
	latch := sync.WaitGroup{}
	defer func() {
		fencingContextCancel()
		latch.Wait()
		fenceQuorumTimer.Done()
	}()

	// Channel to receive responses or errors from each server
	ch := make(chan struct {
		DataServer *proto.DataServerIdentity
		EntryID    *proto.EntryId
		Err        error
	}, fencingQuorumSize)

	for _, server := range fencingDataServers {
		// We need to save the address because it gets modified in the eventLoop
		pinedServer := server
		latch.Go(func() {
			process.DoWithLabels(
				fencingContext,
				map[string]string{
					"oxia":        "election-fence-new-term",
					"shard":       fmt.Sprintf("%d", e.shard),
					"data-server": pinedServer.GetNameOrDefault(),
				}, func() {
					entryId, err := e.fenceNewTerm(fencingContext, term, pinedServer)
					if err != nil {
						e.Warn("FenceNewTerm failed", slog.Any("error", err), slog.Any("data-server", pinedServer))
					} else {
						e.Info("Processed fenceNewTerm response", slog.Any("data-server", pinedServer), slog.Any("entry-id", entryId))
					}
					ch <- struct {
						DataServer *proto.DataServerIdentity
						EntryID    *proto.EntryId
						Err        error
					}{DataServer: pinedServer, EntryID: entryId, Err: err}
				},
			)
		})
	}
	candidatesResponse, totalResponses, err := e.waitForMajority(ch, fencingQuorumSize, majority, ensemble)
	if err != nil {
		return nil, err
	}
	e.waitForGracePeriod(ch, fencingQuorumSize, ensemble, totalResponses, candidatesResponse)
	return candidatesResponse, nil
}

func (*ShardElection) waitForGracePeriod(ch chan struct {
	DataServer *proto.DataServerIdentity
	EntryID    *proto.EntryId
	Err        error
}, fencingQuorumSize int, ensemble []*proto.DataServerIdentity, totalResponses int, candidatesResponse map[*proto.DataServerIdentity]*proto.EntryId) {
	// If we have already reached a quorum of successful responses, we can wait a
	// tiny bit more, to allow time for all the "healthy" data servers to respond.
	for totalResponses < fencingQuorumSize {
		select {
		case r := <-ch:
			totalResponses++
			if r.Err != nil {
				// rpc has already printed the logs
				continue
			}
			if slices.Contains(ensemble, r.DataServer) {
				candidatesResponse[r.DataServer] = r.EntryID
			}
		case <-time.After(quorumFencingGracePeriod):
			return
		}
	}
}

func (*ShardElection) waitForMajority(ch chan struct {
	DataServer *proto.DataServerIdentity
	EntryID    *proto.EntryId
	Err        error
}, fencingQuorumSize int, majority int, ensemble []*proto.DataServerIdentity) (map[*proto.DataServerIdentity]*proto.EntryId, int, error) {
	res := make(map[*proto.DataServerIdentity]*proto.EntryId)
	successResponses := 0
	totalResponses := 0
	var err error
	// Wait for a majority to respond
	for successResponses < majority && totalResponses < fencingQuorumSize {
		fencingResponse := <-ch

		totalResponses++
		if fencingResponse.Err != nil {
			err = multierr.Append(err, fencingResponse.Err)
			continue
		}
		successResponses++
		// We don't consider the removed data servers as candidates for leader/followers
		if slices.Contains(ensemble, fencingResponse.DataServer) {
			res[fencingResponse.DataServer] = fencingResponse.EntryID
		}
	}
	if successResponses < majority {
		return nil, totalResponses, errors.Wrap(err, "election failed: quorum not reached")
	}
	return res, totalResponses, nil
}

func (e *ShardElection) selectNewLeader(candidatesStatus map[*proto.DataServerIdentity]*proto.EntryId) (
	leader *proto.DataServerIdentity, followers map[*proto.DataServerIdentity]*proto.EntryId, err error) {
	candidates := chooseCandidates(candidatesStatus)
	server, err := e.leaderSelector.Select(&leaderselector.Context{
		Candidates: candidates,
		Status:     e.metadataStore.LoadStatus(),
	})
	if err != nil {
		return nil, nil, err
	}
	leader = server
	followers = make(map[*proto.DataServerIdentity]*proto.EntryId)
	for a, e := range candidatesStatus {
		if a.GetNameOrDefault() != leader.GetNameOrDefault() {
			followers[a] = e
		}
	}
	return leader, followers, nil
}

func (e *ShardElection) becomeLeader(term int64, leader *proto.DataServerIdentity, followers map[*proto.DataServerIdentity]*proto.EntryId,
	replicationFactor uint32, negotiateFeatures []proto.Feature) error {
	becomeLeaderTimer := e.becomeLeaderLatency.Timer()

	followersMap := make(map[string]*proto.EntryId)
	for server, e := range followers {
		followersMap[server.GetInternal()] = e
	}
	if _, err := e.provider.BecomeLeader(e.Context, leader, &proto.BecomeLeaderRequest{
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

// ensureFollowerCaught Must be called before we change ensemble to avoid any potential data lost.
func (e *ShardElection) ensureFollowerCaught(ensemble []*proto.DataServerIdentity, leader *proto.DataServerIdentity, leaderEntry *proto.EntryId) {
	waitGroup := sync.WaitGroup{}
	for _, server := range ensemble {
		if server.GetNameOrDefault() == leader.GetNameOrDefault() {
			continue
		}
		waitGroup.Add(1)
		go process.DoWithLabels(
			e.Context,
			map[string]string{
				"oxia":        "shard-caught-up-monitor",
				"namespace":   e.namespace,
				"shard":       fmt.Sprintf("%d", e.shard),
				"data-server": server.GetNameOrDefault(),
			}, func() {
				defer waitGroup.Done()
				err := backoff.RetryNotify(func() error {
					fs, err := e.provider.GetStatus(e.Context, server, &proto.GetStatusRequest{Shard: e.shard})
					if err != nil {
						return err
					}
					followerHeadOffset := fs.HeadOffset
					if followerHeadOffset >= leaderEntry.Offset {
						e.Info(
							"Follower is caught-up with the leader after election",
							slog.Any("server", server),
						)
						return nil
					}
					e.Info(
						"Follower is *not* caught-up yet with the leader",
						slog.Any("server", server),
						slog.Int64("leader-head-offset", leaderEntry.Offset),
						slog.Int64("follower-head-offset", followerHeadOffset),
					)
					return ErrFollowerNotCaughtUp
				}, oxiatime.NewBackOff(e.Context), func(err error, duration time.Duration) {
					switch {
					case errors.Is(err, ErrFollowerNotCaughtUp):
					case status.Code(err) == constant.CodeNodeIsNotMember:
						e.Info("Follower has not been added by leader yet",
							slog.Any("server", server),
							slog.Int64("shard", e.shard),
							slog.Duration("retry-after", duration),
						)
					default:
						e.Warn("Failed to get the follower status",
							slog.Any("server", server),
							slog.Any("error", err),
							slog.Duration("retry-after", duration),
						)
					}
				})
				if err != nil {
					e.Info("Abort data server swap follower status validation due to context canceled", slog.Any("error", err))
					return
				}
			})
	}
	waitGroup.Wait()
}

func (e *ShardElection) fenceNewTermAndAddFollower(ctx context.Context, term int64, leader *proto.DataServerIdentity, follower *proto.DataServerIdentity) error {
	fr, err := e.fenceNewTerm(ctx, term, follower)
	if err != nil {
		return err
	}

	if _, err := e.provider.AddFollower(ctx, leader, &proto.AddFollowerRequest{
		Namespace:    e.namespace,
		Shard:        e.shard,
		Term:         term,
		FollowerName: follower.GetInternal(),
		FollowerHeadEntryId: &proto.EntryId{
			Term:   fr.Term,
			Offset: fr.Offset,
		},
	}); err != nil {
		return err
	}

	e.Info(
		"Successfully rejoined the quorum",
		slog.Any("follower", follower),
		slog.Int64("term", fr.Term),
	)
	return nil
}

func (e *ShardElection) fencingFailedFollowers(term int64, ensemble []*proto.DataServerIdentity, leader *proto.DataServerIdentity,
	successfulFollowers map[*proto.DataServerIdentity]*proto.EntryId) {
	if len(successfulFollowers) == len(ensemble)-1 {
		e.Debug(
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
		e.Info("Data server has failed in leader election, retrying", slog.Any("follower", follower))
		group.Go(func() {
			process.DoWithLabels(
				e.Context,
				map[string]string{
					"oxia":     "election-retry-failed-follower",
					"shard":    fmt.Sprintf("%d", e.shard),
					"follower": follower.GetNameOrDefault(),
				},
				func() {
					bo := oxiatime.NewBackOffWithInitialInterval(e.Context, 1*time.Second)
					_ = backoff.RetryNotify(func() error {
						var err error
						if err = e.fenceNewTermAndAddFollower(e.Context, term, leader, follower); status.Code(err) == constant.CodeInvalidTerm {
							// If we're receiving invalid term error, it would mean
							// there's already a new term generated, and we don't have
							// to keep trying with this old term
							e.Warn(
								"Failed to fenceNewTermAndAddFollower, invalid term. Stop trying",
								slog.Any("follower", follower),
								slog.Int64("term", term),
							)
							return nil
						}
						return err
					}, bo, func(err error, duration time.Duration) {
						e.Warn(
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

func (e *ShardElection) prepareIfChangeEnsemble(mutShardMeta *proto.ShardMetadata) {
	from := e.changeEnsembleAction.From
	to := e.changeEnsembleAction.To
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
	mutShardMeta.Ensemble = append(slices.DeleteFunc(mutShardMeta.Ensemble, func(dataServer *proto.DataServerIdentity) bool {
		return dataServer.GetNameOrDefault() == from.GetNameOrDefault()
	}), to)
	e.Info(
		"Changing ensemble",
		slog.Any("removed-data-servers", mutShardMeta.RemovedNodes),
		slog.Any("new-ensemble", mutShardMeta.Ensemble),
		slog.Any("from", from),
		slog.Any("to", to),
	)
}

func (e *ShardElection) start() (*proto.DataServerIdentity, error) {
	e.Info("Starting a new election")
	timer := e.leaderElectionLatency.Timer()

	mutShardMeta := e.meta.Compute(func(metadata *proto.ShardMetadata) {
		metadata.Status = proto.ShardStatusElection
		metadata.Leader = nil
		metadata.Term++
		metadata.Ensemble = e.refreshedEnsemble(metadata.Ensemble)
	})
	e.metadataStore.UpdateShardMetadata(e.namespace, e.shard, mutShardMeta)

	if e.changeEnsembleAction != nil {
		e.prepareIfChangeEnsemble(mutShardMeta)
	}

	// Send NewTerm to all the ensemble members
	candidatesStatus, err := e.fenceNewTermQuorum(mutShardMeta.Term, mutShardMeta.Ensemble, mutShardMeta.RemovedNodes)
	if err != nil {
		return nil, err
	}
	newLeader, followers, err := e.selectNewLeader(candidatesStatus)
	if err != nil {
		return nil, err
	}
	if e.Enabled(context.Background(), slog.LevelInfo) {
		f := make([]struct {
			ServerAddress *proto.DataServerIdentity `json:"server-address"`
			EntryId       *proto.EntryId            `json:"entry-id"`
		}, 0)
		for sa, entryId := range followers {
			f = append(f, struct {
				ServerAddress *proto.DataServerIdentity `json:"server-address"`
				EntryId       *proto.EntryId            `json:"entry-id"`
			}{ServerAddress: sa, EntryId: entryId})
		}
		e.Info(
			"Successfully moved ensemble to a new term",
			slog.Int64("term", mutShardMeta.Term),
			slog.Any("new-leader", newLeader),
			slog.Any("followers", f),
		)
	}
	features := e.dataServerSupportedFeaturesSupplier(mutShardMeta.Ensemble)
	negotiatedFeatures := negotiate(features)

	if err = e.becomeLeader(mutShardMeta.Term, newLeader, followers,
		uint32(len(mutShardMeta.Ensemble)), negotiatedFeatures); err != nil {
		return nil, err
	}
	mutShardMeta.Status = proto.ShardStatusSteadyState
	mutShardMeta.PendingDeleteShardNodes = slices.Concat(mutShardMeta.PendingDeleteShardNodes, mutShardMeta.RemovedNodes)
	mutShardMeta.RemovedNodes = nil
	mutShardMeta.Leader = newLeader

	term := mutShardMeta.Term
	ensemble := mutShardMeta.Ensemble
	leader := mutShardMeta.Leader
	leaderEntry := candidatesStatus[leader]

	e.metadataStore.UpdateShardMetadata(e.namespace, e.shard, mutShardMeta)
	e.meta.Store(mutShardMeta)
	if e.eventListener != nil {
		e.eventListener.LeaderElected(e.shard, newLeader, maps.Keys(followers))
	}

	e.Info(
		"Elected new leader",
		slog.Int64("term", mutShardMeta.Term),
		slog.Any("leader", mutShardMeta.Leader),
		slog.Any("ensemble", mutShardMeta.Ensemble),
	)
	timer.Done()

	e.Go(func() {
		process.DoWithLabels(
			e.Context,
			map[string]string{
				"oxia":  "election-fencing-failed-followers",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				e.fencingFailedFollowers(term, ensemble, leader, followers)
			},
		)
	})
	e.Go(func() {
		process.DoWithLabels(
			e.Context,
			map[string]string{
				"oxia":  "election-monitor-followers-caught-up",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				e.ensureFollowerCaught(ensemble, leader, leaderEntry)

				e.followerCaughtUp.Store(true)
			},
		)
	})
	return newLeader, nil
}

func negotiate(nodeFeatures map[string][]proto.Feature) []proto.Feature {
	if len(nodeFeatures) == 0 {
		return nil
	}

	// Start with all supported features
	featureCount := make(map[proto.Feature]int)
	nodeCount := len(nodeFeatures)

	for _, features := range nodeFeatures {
		// Track unique features per node to handle duplicates
		seen := make(map[proto.Feature]bool)
		for _, f := range features {
			if f == proto.Feature_FEATURE_UNKNOWN {
				continue
			}
			if !seen[f] {
				seen[f] = true
				featureCount[f]++
			}
		}
	}

	// Only include features supported by ALL nodes
	var negotiated []proto.Feature
	for feature, count := range featureCount {
		if count == nodeCount {
			negotiated = append(negotiated, feature)
		}
	}

	return negotiated
}

func (e *ShardElection) IsReadyForChangeEnsemble() bool {
	return e.followerCaughtUp.Load()
}

func (e *ShardElection) Start() *proto.DataServerIdentity {
	if swapped := e.started.CompareAndSwap(false, true); !swapped {
		panic("bug! the election has been started")
	}
	newLeader, _ := backoff.RetryNotifyWithData[*proto.DataServerIdentity](func() (*proto.DataServerIdentity, error) {
		return e.start()
	}, oxiatime.NewBackOff(e.Context), func(err error, duration time.Duration) {
		e.leaderElectionsFailed.Inc()
		e.Warn(
			"Leader election has failed, retrying later",
			slog.Int64("term", e.meta.Term()),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	return newLeader
}

func (e *ShardElection) Stop() {
	e.CancelFunc()
	e.Wait()
	e.Info("stopped the election", slog.Any("term", e.meta.Term()))
}

//nolint:revive
func NewShardElection(ctx context.Context, logger *slog.Logger, eventListener ShardEventListener,
	metadataStore coordmetadata.Metadata,
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier,
	leaderSelector selector.Selector[*leaderselector.Context, *proto.DataServerIdentity],
	provider rpc.Provider, metadata *Metadata, namespace string, shard int64,
	changeEnsembleAction *action.ChangeEnsembleAction, termOptions *proto.NewTermOptions,
	leaderElectionLatency metric.LatencyHistogram, newTermQuorumLatency metric.LatencyHistogram,
	becomeLeaderLatency metric.LatencyHistogram, leaderElectionsFailed metric.Counter) *ShardElection {
	current, cancelFunc := context.WithCancel(ctx)
	return &ShardElection{
		Logger:                              logger,
		WaitGroup:                           sync.WaitGroup{},
		Context:                             current,
		CancelFunc:                          cancelFunc,
		metadataStore:                       metadataStore,
		dataServerSupportedFeaturesSupplier: dataServerSupportedFeaturesSupplier,
		eventListener:                       eventListener,
		leaderSelector:                      leaderSelector,
		meta:                                metadata,
		provider:                            provider,
		namespace:                           namespace,
		shard:                               shard,
		termOptions:                         termOptions,
		changeEnsembleAction:                changeEnsembleAction,
		leaderElectionLatency:               leaderElectionLatency,
		newTermQuorumLatency:                newTermQuorumLatency,
		becomeLeaderLatency:                 becomeLeaderLatency,
		leaderElectionsFailed:               leaderElectionsFailed,
	}
}

func chooseCandidates(candidatesStatus map[*proto.DataServerIdentity]*proto.EntryId) []*proto.DataServerIdentity {
	// Select all the data servers that have the highest term first
	var currentMaxTerm int64 = -1
	// Select all the data servers that have the highest entry in the wal
	var currentMax int64 = -1
	var candidates []*proto.DataServerIdentity
	for addr, headEntryId := range candidatesStatus {
		if headEntryId.Term > currentMaxTerm {
			// the new max
			currentMaxTerm = headEntryId.Term
			currentMax = headEntryId.Offset
			candidates = []*proto.DataServerIdentity{addr}
		} else if headEntryId.Term == currentMaxTerm {
			if headEntryId.Offset > currentMax {
				// the new max
				currentMax = headEntryId.Offset
				candidates = []*proto.DataServerIdentity{addr}
			} else if headEntryId.Offset == currentMax {
				candidates = append(candidates, addr)
			}
		}
	}
	return candidates
}
