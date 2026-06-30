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

package commons

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestWriteStructuredOutputJSONUsesProtoEnumNames(t *testing.T) {
	serverName := "server-1"
	view := &proto.DataServerView{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName,
				Public:   "public-1",
				Internal: "internal-1",
			},
		},
		DataServerStatus: &proto.DataServerStatus{
			State:             proto.DataServerState_DATA_SERVER_STATE_RUNNING,
			SupportedFeatures: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
		},
	}

	out := new(bytes.Buffer)
	require.NoError(t, WriteStructuredOutput(out, OutputJSON, view))

	assert.Contains(t, out.String(), `"DATA_SERVER_STATE_RUNNING"`)
	assert.Contains(t, out.String(), `"FEATURE_DB_CHECKSUM"`)
	assert.NotContains(t, out.String(), `"state": 1`)
}

func TestWriteStructuredOutputJSONUsesProtoEnumNamesForSlices(t *testing.T) {
	views := []*proto.DataServerView{{
		DataServerStatus: &proto.DataServerStatus{
			State: proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		},
	}}

	out := new(bytes.Buffer)
	require.NoError(t, WriteStructuredOutput(out, OutputJSON, views))

	assert.Contains(t, out.String(), `"DATA_SERVER_STATE_DRAINING"`)
	assert.NotContains(t, out.String(), `"state": 3`)
}
