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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestCandidatesLogValueGroupsByName(t *testing.T) {
	name := "s1"
	entryId := &proto.EntryId{Term: 2, Offset: 5}
	value := Candidates{{Name: &name, Public: "s1:9091", Internal: "s1:8191"}: {HeadEntryId: entryId}}.LogValue()

	assert.Equal(t, slog.KindGroup, value.Kind())
	assert.Equal(t, []slog.Attr{slog.Any("s1", entryId)}, value.Group())
}

func TestCandidatesMoveMovesCandidate(t *testing.T) {
	s1 := &proto.DataServerIdentity{Public: "s1", Internal: "s1"}
	s2 := &proto.DataServerIdentity{Public: "s2", Internal: "s2"}
	s3 := &proto.DataServerIdentity{Public: "s3", Internal: "s3"}
	from := Candidates{s1: {HeadEntryId: &proto.EntryId{Term: 2, Offset: 5}}, s2: {HeadEntryId: &proto.EntryId{Term: 2, Offset: 4}}}
	to := Candidates{s3: {HeadEntryId: &proto.EntryId{Term: 1, Offset: 9}}}

	from.Move(s1, to)

	assert.Equal(t, Candidates{s2: {HeadEntryId: &proto.EntryId{Term: 2, Offset: 4}}}, from)
	assert.Equal(t, Candidates{s3: {HeadEntryId: &proto.EntryId{Term: 1, Offset: 9}}, s1: {HeadEntryId: &proto.EntryId{Term: 2, Offset: 5}}}, to)
}
