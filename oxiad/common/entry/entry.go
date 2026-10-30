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

package entry

import (
	"cmp"

	"github.com/oxia-db/oxia/common/proto"
)

// Compare orders the entry ids by term, then by offset.
func Compare(a *proto.EntryId, b *proto.EntryId) int {
	return cmp.Or(cmp.Compare(a.GetTerm(), b.GetTerm()), cmp.Compare(a.GetOffset(), b.GetOffset()))
}

// NoOpValue is the value of an entry without any operation: a write request
// without any operation, which only advances the commit offset. A leader
// appends one for its term, to commit the entries of the earlier terms. It
// must not be modified.
var NoOpValue = &proto.LogEntryValue{Value: &proto.LogEntryValue_Requests{
	Requests: &proto.WriteRequests{Writes: []*proto.WriteRequest{{}}},
}}
