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

package option

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

func TestMetadataOptionsValidateRaftRequiresBootstrapNodes(t *testing.T) {
	opts := MetadataOptions{
		ProviderName: metadata.ProviderNameRaft,
		Raft: RaftMetadata{
			Address: "127.0.0.1:6645",
			DataDir: "/tmp/raft",
		},
	}

	err := opts.Validate()
	assert.EqualError(t, err, "raft-bootstrap-nodes must be set with metadata=raft")
}

func TestMetadataOptionsValidateRaftRejectsDuplicateBootstrapNodes(t *testing.T) {
	opts := MetadataOptions{
		ProviderName: metadata.ProviderNameRaft,
		Raft: RaftMetadata{
			Address:        "127.0.0.1:6645",
			DataDir:        "/tmp/raft",
			BootstrapNodes: []string{"127.0.0.1:6645", "127.0.0.1:6645"},
		},
	}

	err := opts.Validate()
	assert.EqualError(t, err, `raft-bootstrap-nodes contains duplicate node "127.0.0.1:6645"`)
}

func TestMetadataOptionsValidateRaftRequiresLocalAddressInBootstrapNodes(t *testing.T) {
	opts := MetadataOptions{
		ProviderName: metadata.ProviderNameRaft,
		Raft: RaftMetadata{
			Address:        "127.0.0.1:6645",
			DataDir:        "/tmp/raft",
			BootstrapNodes: []string{"127.0.0.1:6646"},
		},
	}

	err := opts.Validate()
	assert.EqualError(t, err, `raft-address "127.0.0.1:6645" must be included in raft-bootstrap-nodes`)
}
