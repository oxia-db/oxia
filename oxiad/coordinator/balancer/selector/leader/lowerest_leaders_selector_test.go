// Copyright 2023-2025 The Oxia Authors
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

package leader

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
)

func TestSelect_NoCandidates(t *testing.T) {
	s := NewSelector()

	_, err := s.Select(&Context{
		Candidates: []*proto.DataServerIdentity{},
		Status:     proto.NewClusterStatus(),
	})
	assert.ErrorIs(t, err, selector.ErrNoCandidates)
}

func TestSelect_NilCandidates(t *testing.T) {
	s := NewSelector()

	_, err := s.Select(&Context{
		Candidates: nil,
		Status:     proto.NewClusterStatus(),
	})
	assert.ErrorIs(t, err, selector.ErrNoCandidates)
}

func TestSelect_SingleCandidate(t *testing.T) {
	s := NewSelector()

	server, err := s.Select(&Context{
		Candidates: []*proto.DataServerIdentity{
			{Internal: "127.0.0.1:6601", Public: "127.0.0.1:6611"},
		},
		Status: proto.NewClusterStatus(),
	})
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:6601", server.Internal)
}
