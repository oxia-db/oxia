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

// HeadEntry is what a data server reports when it gets fenced: its head entry,
// and its commit offset, or nil when it reports none.
type HeadEntry struct {
	*proto.EntryId
	CommitOffset *int64
}

// HeadEntries are the head entries of the data servers of a shard.
type HeadEntries map[*proto.DataServerIdentity]HeadEntry

// LogValue logs the head entries by the name of their data server.
func (h HeadEntries) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, len(h))
	for dataServer, head := range h {
		attrs = append(attrs, slog.Any(dataServer.GetNameOrDefault(), head.EntryId))
	}
	return slog.GroupValue(attrs...)
}

// Move moves the head entry of a data server to the other head entries.
func (h HeadEntries) Move(server *proto.DataServerIdentity, to HeadEntries) {
	to[server] = h[server]
	delete(h, server)
}
