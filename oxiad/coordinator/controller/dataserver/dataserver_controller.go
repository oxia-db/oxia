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

package dataserver

import (
	"io"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

// Controller manages one data server for the coordinator. It:
//
//   - checks the health of the data server, with a watch stream and periodic
//     probes, and tells the listener when the data server becomes unavailable;
//   - binds a data server that becomes available to this coordinator with a
//     handshake, which reports the features it supports, and tells the
//     listener when they change;
//   - pushes the shard assignments to the data server while it is available
//     and RUNNING or DRAINING, retrying with a backoff when the stream fails;
//   - once the data server is DRAINING, checks on every probe interval
//     whether it is drained, and stores it DRAINED.
//
// The status of the data server has two parts. Its state is the stage of its
// lifecycle, which the metadata stores, so that a drain is reported, and
// resumes after a restart of the coordinator:
//
//	        Drain                 no shard references it,
//	RUNNING -------> DRAINING ------------------------------> DRAINED
//	                           and every shard caught up
//
// Its availability is observed by this controller only, and never stored: a
// data server becomes available with a handshake, at any stage, and
// unavailable when its health check fails. It starts RUNNING and unavailable.
//
// The runtime creates a controller for each data server of the configuration,
// or still referenced by a shard or stored DRAINING. It calls Drain when the
// data server is removed from the configuration, and closes the controller
// once it is DRAINED. A data server added again while it drains keeps its
// controller, and its drain.
type Controller interface {
	// Close stops the health checks and the assignments stream.
	io.Closer

	// GetDataServer returns the data server that the controller manages.
	GetDataServer() commonobject.Borrowed[*proto.DataServer]

	// GetStatus returns a snapshot of the current status of the data server:
	// advancing it doesn't change the data server's.
	GetStatus() Status

	// GetFeatures returns the features that the data server
	// supports, as its last handshake reported them, or none before its
	// first handshake.
	GetFeatures() []proto.Feature

	// Drain moves a data server removed from the configuration to DRAINING:
	// it keeps receiving the shard assignments until it is DRAINED, when the
	// controller tells the listener. It does nothing to a data server already
	// DRAINING or DRAINED.
	Drain()
}
