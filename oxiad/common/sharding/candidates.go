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

package sharding

import (
	"log/slog"

	"github.com/oxia-db/oxia/common/proto"
)

// Candidate is what a data server of a shard reports when it gets fenced with
// a new term: its head entry, and its commit offset, or nil when it reports
// none.
type Candidate struct {
	HeadEntryId  *proto.EntryId
	CommitOffset *int64
}

// Candidates are the candidates of the data servers of a shard.
type Candidates map[*proto.DataServerIdentity]Candidate

// LogValue logs the head entries of the candidates by the name of their data
// server.
func (c Candidates) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, len(c))
	for dataServer, candidate := range c {
		attrs = append(attrs, slog.Any(dataServer.GetNameOrDefault(), candidate.HeadEntryId))
	}
	return slog.GroupValue(attrs...)
}

// Move moves the candidate of a data server to the other candidates.
func (c Candidates) Move(server *proto.DataServerIdentity, to Candidates) {
	to[server] = c[server]
	delete(c, server)
}
