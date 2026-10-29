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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSameDataServerMatchesByName(t *testing.T) {
	name := "s1"
	s1 := &proto.DataServerIdentity{Name: &name, Public: "s1:9091", Internal: "s1:8191"}
	moved := &proto.DataServerIdentity{Name: &name, Public: "s1-new:9091", Internal: "s1-new:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}

	assert.True(t, SameDataServer(s1)(moved))
	assert.False(t, SameDataServer(s1)(s2))
}
