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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestAntiAffinityRejectsASharedZone(t *testing.T) {
	s := snapshot()
	s.Namespaces["a"].AntiAffinities = zones
	assert.NoError(t, AntiAffinity{}.Filter(s, move("n0", "n2")))

	s.DataServers["n2"].Labels["zone"] = "z1"

	assert.ErrorIs(t, AntiAffinity{}.Filter(s, move("n0", "n2")), errs.ErrFailedPrecondition)
	assert.NoError(t, AntiAffinity{}.Filter(s, model.Op{Kind: model.TransferLeader, Namespace: "a", Shard: 1,
		From: "n0", To: "n1"}))
}

func TestConflicts(t *testing.T) {
	s := snapshot()
	members := []string{"n0", "n1"}

	assert.False(t, Conflicts(s, nil, members, "n0", "n3"))
	assert.True(t, Conflicts(s, zones, members, "n0", "n3"), "the target lacks the label")
	assert.False(t, Conflicts(s, zones, members, "n0", "n2"))
	s.DataServers["n2"].Labels["zone"] = "z0"
	assert.False(t, Conflicts(s, zones, members, "n0", "n2"), "it replaces the member it shares a zone with")
	assert.True(t, Conflicts(s, zones, members, "n1", "n2"))
}

var zones = []*proto.AntiAffinity{{Labels: []string{"zone"}}}
