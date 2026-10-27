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

package coordinator

import (
	"io"
)

// Coordinator is a running Oxia coordinator, which [New] starts once it holds
// the metadata leadership. It serves the health checks on its internal address
// and the admin API on its public address, and coordinates the cluster: it
// reconciles the cluster configuration, assigns the shards to the data
// servers, elects their leaders, and balances and splits them.
//
// A coordinator that loses the metadata leadership exits the process, so that
// it can't act alongside the new leader: a restart rejoins the election. Only
// the raft and configmap metadata providers can lose it.
type Coordinator interface {
	// Close stops the coordination and the servers, and releases the metadata.
	// It releases them once: a later call, or a concurrent one, returns nil.
	io.Closer
}
