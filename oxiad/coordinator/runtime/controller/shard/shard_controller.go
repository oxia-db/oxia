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
	"io"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

// FeaturesSupplier returns the features that each data server supports, by
// name, as their handshakes reported them.
type FeaturesSupplier = func(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature

// NoOpFeaturesSupplier reports no features for any data server.
func NoOpFeaturesSupplier(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature, len(dataServers))
	for _, dataServer := range dataServers {
		features[dataServer.GetNameOrDefault()] = []proto.Feature{}
	}
	return features
}

// Controller manages one shard for the coordinator. A single event loop applies
// the requests and the notifications about the shard, one at a time:
//
//	runtime, balancer                        data server controllers
//	   | Dispatch, Delete,                      | BecameUnavailable,
//	   | Refresh                                | FeaturesDiscovered
//	   v                                        v
//	+--------------------------- event loop ----------------------------+
//	| elect a leader      when the leader fails, the balancer moves it, |
//	|                     an address changes, or the ensemble supports  |
//	|                     features that the term doesn't pin            |
//	| change the ensemble with a new election                           |
//	| delete the shard    from its ensemble, then its status            |
//	| periodic tasks      delete the shard from the removed members,    |
//	|                     and check the features of the term            |
//	+-------------------------------------------------------------------+
//	   |
//	   v the shard status (metadata), the data servers, ShardEventListener
//
// When it starts, the controller deletes a shard marked Deleting, waits for
// the split of a child shard to complete, elects a leader if the shard has no
// healthy one, or verifies the ensemble of the current leader. It retries the
// start until it is closed.
//
// The methods queue their work for the event loop, and don't wait for it. A
// request that arrives while the controller is closing, or after the shard is
// deleted, is dropped or fails.
//
// The runtime creates a controller for each shard of a namespace, and for each
// child shard of a split, and closes it when the shard is deleted, when its
// namespace is removed, or when the runtime closes.
type Controller interface {
	// Close stops the event loop and the current election, and fails the
	// actions still queued with constant.ErrResourceUnavailable.
	io.Closer

	// DataServerEventListener receives the notifications of the data server
	// controllers. The failure of the leader starts a new election, and the
	// discovery of the features of an ensemble member checks the features
	// pinned by the term. A failure notification is dropped when the queue
	// is full; discovered features are kept until the event loop handles them.
	controllerapi.DataServerEventListener

	// Refresh brings the shard up to date with the cluster configuration
	// after it changes. Today, it starts a new election if the address of an
	// ensemble member changed, so that the members get the new address. It
	// doesn't wait for the election, and drops it when the queue is full.
	Refresh()

	// Delete deletes the shard from the data servers of its ensemble,
	// then its status, and calls ShardDeleted on the listener, retrying every
	// second until it succeeds. It doesn't wait for the deletion.
	Delete()

	// Dispatch queues an action for the event loop, which completes it with
	// its result. It panics on an action of another type:
	//
	//   - *action.ElectionAction elects a new leader of the shard, e.g. to move
	//     the leadership. It completes with the name of the new leader, and
	//     fails with ErrElectionFailed if the election ended without
	//     electing one, e.g. because a newer election replaced it, and with
	//     constant.ErrResourceUnavailable if the queue was full or the
	//     controller is closing.
	//   - *action.ChangeEnsembleAction replaces a member of the ensemble of the
	//     shard with a new election. It fails with ErrInvalidChangeEnsemble if
	//     the replaced member isn't in the ensemble, or the new one already is;
	//     ErrNotReadyForChangeEnsemble while the followers of the current
	//     leader catch up, the shard is part of a split, or the features of a
	//     member aren't known yet; ErrChangeEnsembleLosesFeatureSupport if the
	//     new ensemble doesn't support the features that the current one
	//     negotiated; and constant.ErrResourceUnavailable while the controller
	//     is closing.
	//
	// It doesn't wait for the action, only for room in the queue of the
	// changes of the ensemble.
	Dispatch(ac action.Action)
}
