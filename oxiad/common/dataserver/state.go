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

import "github.com/oxia-db/oxia/common/proto"

// IsRemoved reports whether a data server in the state was removed from the
// cluster: it is DRAINING, or DRAINED.
func IsRemoved(state proto.DataServerState) bool {
	return state == proto.DataServerState_DATA_SERVER_STATE_DRAINING ||
		state == proto.DataServerState_DATA_SERVER_STATE_DRAINED
}

// NeedsRunning reports whether a data server of the configuration, in the
// stored state, must be stored RUNNING: it has no stored state, or it is
// DRAINED and was added again. A DRAINING one added again keeps draining.
func NeedsRunning(state proto.DataServerState) bool {
	return state == proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED ||
		state == proto.DataServerState_DATA_SERVER_STATE_DRAINED
}
