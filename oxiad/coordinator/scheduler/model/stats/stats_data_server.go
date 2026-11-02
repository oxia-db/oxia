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

package stats

import "github.com/oxia-db/oxia/common/proto"

type DataServerStats struct {
	*proto.DataServer
	Removed bool
	Down    bool
	// Copying counts the replicas copied onto the data server: the changes of
	// ensembles whose new member it is, until the ensemble catches up.
	Copying int
}
