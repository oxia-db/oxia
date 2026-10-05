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

package runtime

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"
)

// instanceIDFailingMetadata cannot load the instance id.
type instanceIDFailingMetadata struct {
	coordmetadata.Metadata
}

func (instanceIDFailingMetadata) GetInstanceID() (string, error) {
	return "", errors.New("status unavailable")
}

// The runtime does not start without an instance id.
func TestNewFailsWithoutInstanceID(t *testing.T) {
	metadata := instanceIDFailingMetadata{Metadata: newTestMetadata(t, &proto.ClusterConfiguration{})}
	_, err := New(metadata, func(string) rpc.Provider { return mockutils.NewRpcProvider() })
	require.ErrorContains(t, err, "status unavailable")
}
