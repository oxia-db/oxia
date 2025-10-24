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
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/resources"
	"github.com/oxia-db/oxia/coordinator/rpc"
	"github.com/oxia-db/oxia/coordinator/selectors"
	leaderselector "github.com/oxia-db/oxia/coordinator/selectors/leader"
	"github.com/oxia-db/oxia/proto"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	"golang.org/x/net/context"
	"google.golang.org/grpc/status"
)

type Election struct {
	*slog.Logger
	sync.WaitGroup
	context.Context
	context.CancelFunc
	// metrics
	leaderElectionLatency metric.LatencyHistogram
	newTermQuorumLatency  metric.LatencyHistogram
	becomeLeaderLatency   metric.LatencyHistogram
	leaderElectionsFailed metric.Counter
	// borrowed resources
	statusResource resources.StatusResource
	configResource resources.ClusterConfigResource
	leaderSelector selectors.Selector[*leaderselector.Context, model.Server]
	eventListener  ShardEventListener
	provider       rpc.Provider
	meta           *Metadata
	// owned status
	namespace   string
	shard       int64
	termOptions *proto.NewTermOptions
}

func (e *Election) refreshedEnsemble(ensemble []model.Server) []model.Server {
	refreshedEnsembleNodeAddress := make([]model.Server, len(ensemble))
	for idx, candidate := range ensemble {
		if refreshedAddress, exist := e.configResource.Node(candidate.GetIdentifier()); exist {
			refreshedEnsembleNodeAddress[idx] = *refreshedAddress
			continue
		}
		refreshedEnsembleNodeAddress[idx] = candidate
	}
	if e.Logger.Enabled(e.Context, slog.LevelDebug) {
		if !reflect.DeepEqual(ensemble, refreshedEnsembleNodeAddress) {
			e.Logger.Info("refresh the shard ensemble node address", slog.Any("current-ensemble", ensemble),
				slog.Any("new-ensemble", refreshedEnsembleNodeAddress))
		}
	}
	return refreshedEnsembleNodeAddress
}

func (e *Election) fenceNewTerm(ctx context.Context, term int64, node model.Server) (*proto.EntryId, error) {
	res, err := e.provider.NewTerm(ctx, node, &proto.NewTermRequest{
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
func (e *Election) fenceNewTermQuorum(term int64, ensemble []model.Server, removedCandidates []model.Server) (map[model.Server]*proto.EntryId, error) {
	fenceQuorumTimer := e.newTermQuorumLatency.Timer()

	fencingNodes := slices.Concat(ensemble, removedCandidates)
	fencingQuorumSize := len(fencingNodes)
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
		model.Server
		*proto.EntryId
		error
	}, fencingQuorumSize)

	for _, server := range fencingNodes {
		// We need to save the address because it gets modified in the eventLoop
		pinedServer := server
		latch.Go(func() {
			process.DoWithLabels(
				fencingContext,
				map[string]string{
					"oxia":  "election-fence-new-term",
					"shard": fmt.Sprintf("%d", e.shard),
					"node":  pinedServer.GetIdentifier(),
				}, func() {
					entryId, err := e.fenceNewTerm(fencingContext, term, pinedServer)
					if err != nil {
						e.Logger.Warn("Failed to fenceNewTerm node", slog.Any("error", err), slog.Any("node", pinedServer))
					} else {
						e.Logger.Info("Processed fenceNewTerm response", slog.Any("node", pinedServer), slog.Any("entry-id", entryId))
					}
					ch <- struct {
						model.Server
						*proto.EntryId
						error
					}{pinedServer, entryId, err}
				},
			)
		})
	}
	successResponses := 0
	totalResponses := 0

	res := make(map[model.Server]*proto.EntryId)
	var err error
	// Wait for a majority to respond
	for successResponses < majority && totalResponses < fencingQuorumSize {
		fencingResponse := <-ch

		totalResponses++
		if fencingResponse.error == nil {
			successResponses++
			// We don't consider the removed nodes as candidates for leader/followers
			if slices.Contains(ensemble, fencingResponse.Server) {
				res[fencingResponse.Server] = fencingResponse.EntryId
			}
		} else {
			err = multierr.Append(err, fencingResponse.error)
		}
	}
	if successResponses < majority {
		return nil, errors.Wrap(err, "failed to fenceNewTerm shard")
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
			return res, nil
		}
	}
	return res, nil
}

func (e *Election) selectNewLeader(candidatesStatus map[model.Server]*proto.EntryId) (
	leader model.Server, followers map[model.Server]*proto.EntryId) {
	// Select all the nodes that have the highest term first
	var currentMaxTerm int64 = -1
	// Select all the nodes that have the highest entry in the wal
	var currentMax int64 = -1
	var candidates []model.Server
	for addr, headEntryId := range candidatesStatus {
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

	server, _ := e.leaderSelector.Select(&leaderselector.Context{
		Candidates: candidates,
		Status:     e.statusResource.Load(),
	})
	leader = server
	followers = make(map[model.Server]*proto.EntryId)
	for a, e := range candidatesStatus {
		if a != leader {
			followers[a] = e
		}
	}
	return leader, followers
}

func (e *Election) becomeLeader(term int64, leader model.Server, followers map[model.Server]*proto.EntryId, replicationFactor uint32) error {
	becomeLeaderTimer := e.becomeLeaderLatency.Timer()

	followersMap := make(map[string]*proto.EntryId)
	for server, e := range followers {
		followersMap[server.Internal] = e
	}
	if _, err := e.provider.BecomeLeader(e.Context, leader, &proto.BecomeLeaderRequest{
		Namespace:         e.namespace,
		Shard:             e.shard,
		Term:              term,
		ReplicationFactor: replicationFactor,
		FollowerMaps:      followersMap,
	}); err != nil {
		return err
	}

	becomeLeaderTimer.Done()
	return nil
}

func (e *Election) fenceNewTermAndAddFollower(ctx context.Context, term int64, leader model.Server, follower model.Server) error {
	fr, err := e.fenceNewTerm(ctx, term, follower)
	if err != nil {
		return err
	}

	if _, err := e.provider.AddFollower(ctx, leader, &proto.AddFollowerRequest{
		Namespace:    e.namespace,
		Shard:        e.shard,
		Term:         term,
		FollowerName: follower.Internal,
		FollowerHeadEntryId: &proto.EntryId{
			Term:   fr.Term,
			Offset: fr.Offset,
		},
	}); err != nil {
		return err
	}

	e.Logger.Info(
		"Successfully rejoined the quorum",
		slog.Any("follower", follower),
		slog.Int64("term", fr.Term),
	)
	return nil
}

func (e *Election) fencingFailedFollowers(term int64, ensemble []model.Server, leader *model.Server,
	successfulFollowers map[model.Server]*proto.EntryId) {
	if len(successfulFollowers) == len(ensemble)-1 {
		e.Logger.Debug(
			"All the member of the ensemble were successfully added",
			slog.Int64("term", term),
		)
		return
	}
	// Identify failed followers
	group := sync.WaitGroup{}
	defer group.Wait()
	for _, follower := range ensemble {
		if follower == *leader {
			continue
		}
		if _, found := successfulFollowers[follower]; found {
			continue
		}
		e.Logger.Info("Node has failed in leader election, retrying", slog.Any("follower", follower))

		group.Add(1)
		go process.DoWithLabels(
			e.Context,
			map[string]string{
				"oxia":     "election-retry-failed-follower",
				"shard":    fmt.Sprintf("%d", e.shard),
				"follower": follower.GetIdentifier(),
			},
			func() {
				defer group.Done()
				_ = backoff.RetryNotify(func() error {
					if err := e.fenceNewTermAndAddFollower(e.Context, term, *leader, follower); status.Code(err) == constant.CodeInvalidTerm {
						// If we're receiving invalid term error, it would mean
						// there's already a new term generated, and we don't have
						// to keep trying with this old term
						e.Logger.Warn(
							"Failed to fenceNewTermAndAddFollower, invalid term. Stop trying",
							slog.Any("follower", follower),
							slog.Int64("term", term),
						)
						return nil
					} else {
						return err
					}
				}, oxiatime.NewBackOffWithInitialInterval(e.Context, 1*time.Second), func(err error, duration time.Duration) {
					e.Logger.Warn(
						"Failed to fenceNewTermAndAddFollower, retrying later",
						slog.Any("error", err),
						slog.Any("follower", follower),
						slog.Int64("term", term),
						slog.Duration("retry-after", duration),
					)
				})
			},
		)
	}
}

// EnsureFollowerCaught Must be called before we change ensemble to avoid any potential data lost
func (e *Election) EnsureFollowerCaught() {
	metadata := e.meta.Load()
	if metadata.Leader == nil {
		return
	}
	// Wait until all followers are caught up.
	// This is done to avoid doing multiple node-swap concurrently, since it would create
	// additional load in the system, while transferring multiple DB snapshots.
	// Get current head offset for leader
	leaderResponse, err := backoff.RetryNotifyWithData[*proto.GetStatusResponse](func() (*proto.GetStatusResponse, error) {
		return e.provider.GetStatus(e.Context, *metadata.Leader, &proto.GetStatusRequest{Shard: e.shard})
	}, oxiatime.NewBackOff(e.Context), func(err error, duration time.Duration) {
		e.Warn("Failed to get status from leader.",
			slog.Any("error", err), slog.Any("retry-after", duration))
	})
	if err != nil {
		e.Info("Abort node swap leader status validation due to context canceled", slog.Any("error", err))
		return
	}

	leaderHeadOffset := leaderResponse.HeadOffset

	waitGroup := sync.WaitGroup{}
	for _, server := range metadata.Ensemble {
		if server.GetIdentifier() == metadata.Leader.GetIdentifier() {
			continue
		}
		waitGroup.Add(1)
		go process.DoWithLabels(
			e.Context,
			map[string]string{
				"oxia":      "shard-caught-up-monitor",
				"namespace": e.namespace,
				"shard":     fmt.Sprintf("%d", e.shard),
			}, func() {
				defer waitGroup.Done()
				err = backoff.RetryNotify(func() error {
					fs, err := e.provider.GetStatus(e.Context, server, &proto.GetStatusRequest{Shard: e.shard})
					if err != nil {
						return err
					}
					followerHeadOffset := fs.HeadOffset
					if followerHeadOffset >= leaderHeadOffset {
						e.Info(
							"Follower is caught-up with the leader after node-swap",
							slog.Any("server", server),
						)
						return nil
					}
					e.Info(
						"Follower is *not* caught-up yet with the leader",
						slog.Any("server", server),
						slog.Int64("leader-head-offset", leaderHeadOffset),
						slog.Int64("follower-head-offset", followerHeadOffset),
					)
					return errors.New("follower not caught up yet")
				}, oxiatime.NewBackOff(e.Context), func(err error, duration time.Duration) {
					e.Warn("Failed to get the follower status. ", slog.Any("error", err.Error()), slog.Any("retry-after", duration))
				})
				if err != nil {
					e.Info("Abort node swap follower status validation due to context canceled", slog.Any("error", err))
					return
				}
			})
	}
	waitGroup.Wait()
	return
}

func (e *Election) start() (model.Server, error) {
	e.Info("Starting a new election")
	timer := e.leaderElectionLatency.Timer()
	newMeta := e.meta.Compute(func(metadata *model.ShardMetadata) {
		metadata.Status = model.ShardStatusElection
		metadata.Leader = nil
		metadata.Term++
		metadata.Ensemble = e.refreshedEnsemble(metadata.Ensemble)
	})
	e.statusResource.UpdateShardMetadata(e.namespace, e.shard, newMeta)

	// Send NewTerm to all the ensemble members
	candidatesStatus, err := e.fenceNewTermQuorum(newMeta.Term, newMeta.Ensemble, newMeta.RemovedNodes)
	if err != nil {
		return model.Server{}, err
	}
	newLeader, followers := e.selectNewLeader(candidatesStatus)
	if e.Logger.Enabled(context.Background(), slog.LevelInfo) {
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
		e.Logger.Info(
			"Successfully moved ensemble to a new term",
			slog.Int64("term", newMeta.Term),
			slog.Any("new-leader", newLeader),
			slog.Any("followers", f),
		)
	}
	if err = e.becomeLeader(newMeta.Term, newLeader, followers, uint32(len(newMeta.Ensemble))); err != nil {
		return model.Server{}, err
	}
	newMeta.Status = model.ShardStatusSteadyState
	newMeta.Leader = &newLeader
	e.statusResource.UpdateShardMetadata(e.namespace, e.shard, newMeta)
	e.meta.Store(newMeta)
	if e.eventListener != nil {
		e.eventListener.LeaderElected(e.shard, newLeader, maps.Keys(followers))
	}
	e.Logger.Info(
		"Elected new leader",
		slog.Int64("term", newMeta.Term),
		slog.Any("leader", newMeta.Leader),
	)
	timer.Done()

	e.WaitGroup.Go(func() {
		process.DoWithLabels(
			e.Context,
			map[string]string{
				"oxia":  "election-fencing-failed-followers",
				"shard": fmt.Sprintf("%d", e.shard),
			}, func() {
				e.fencingFailedFollowers(newMeta.Term, newMeta.Ensemble, newMeta.Leader, followers)
			},
		)

	})
	return newLeader, nil
}

func (e *Election) Start() model.Server {
	newLeader, _ := backoff.RetryNotifyWithData[model.Server](func() (model.Server, error) {
		return e.start()
	}, oxiatime.NewBackOff(e.Context), func(err error, duration time.Duration) {
		e.leaderElectionsFailed.Inc()
		e.Logger.Warn(
			"Leader election has failed, retrying later",
			slog.Int64("term", e.meta.Term()),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	return newLeader
}

func (e *Election) Stop() {
	e.CancelFunc()
	e.Wait()
	e.Info("stopped the election", slog.Any("term", e.meta.Term()))
}

func CreateNewElection(ctx context.Context, logger *slog.Logger, eventListener ShardEventListener,
	statusResource resources.StatusResource, configResource resources.ClusterConfigResource,
	leaderSelector selectors.Selector[*leaderselector.Context, model.Server], provider rpc.Provider,
	metadata *Metadata, namespace string, shard int64, termOptions *proto.NewTermOptions,
	leaderElectionLatency metric.LatencyHistogram, newTermQuorumLatency metric.LatencyHistogram,
	becomeLeaderLatency metric.LatencyHistogram, leaderElectionsFailed metric.Counter) *Election {
	current, cancelFunc := context.WithCancel(ctx)
	return &Election{
		Logger:                logger,
		WaitGroup:             sync.WaitGroup{},
		Context:               current,
		CancelFunc:            cancelFunc,
		statusResource:        statusResource,
		configResource:        configResource,
		eventListener:         eventListener,
		leaderSelector:        leaderSelector,
		meta:                  metadata,
		provider:              provider,
		namespace:             namespace,
		shard:                 shard,
		termOptions:           termOptions,
		leaderElectionLatency: leaderElectionLatency,
		newTermQuorumLatency:  newTermQuorumLatency,
		becomeLeaderLatency:   becomeLeaderLatency,
		leaderElectionsFailed: leaderElectionsFailed,
	}
}
