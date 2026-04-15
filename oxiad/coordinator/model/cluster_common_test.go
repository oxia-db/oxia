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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerGetIdentifier(t *testing.T) {
	name := "server-1"
	emptyName := ""

	assert.Equal(t, "server-1", (&Server{
		Name:     &name,
		Internal: "internal-1",
	}).GetIdentifier())

	assert.Equal(t, "internal-2", (&Server{
		Internal: "internal-2",
	}).GetIdentifier())

	assert.Equal(t, "internal-3", (&Server{
		Name:     &emptyName,
		Internal: "internal-3",
	}).GetIdentifier())
}

func TestClusterConfigPatchDataServerUpdatesNamedServer(t *testing.T) {
	name := "server-1"
	publicAddress := "public-2"
	internalAddress := "internal-2"

	config := ClusterConfig{
		Servers: []Server{
			{Name: &name, Public: "public-1", Internal: "internal-1"},
		},
		ServerMetadata: map[string]ServerMetadata{
			name: {Labels: map[string]string{"rack": "r1"}},
		},
	}

	server, err := config.PatchDataServer(name, DataServerPatch{
		PublicAddress:   &publicAddress,
		InternalAddress: &internalAddress,
		Metadata: &ServerMetadata{
			Labels: map[string]string{"rack": "r2", "zone": "z1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, server)
	assert.Equal(t, publicAddress, server.Public)
	assert.Equal(t, internalAddress, server.Internal)
	assert.Equal(t, map[string]string{"rack": "r2", "zone": "z1"}, config.ServerMetadata[name].Labels)
}

func TestClusterConfigPatchDataServerClearsMetadata(t *testing.T) {
	name := "server-1"

	config := ClusterConfig{
		Servers: []Server{
			{Name: &name, Public: "public-1", Internal: "internal-1"},
		},
		ServerMetadata: map[string]ServerMetadata{
			name: {Labels: map[string]string{"rack": "r1"}},
		},
	}

	_, err := config.PatchDataServer(name, DataServerPatch{
		Metadata: &ServerMetadata{Labels: map[string]string{}},
	})
	require.NoError(t, err)
	assert.Empty(t, config.ServerMetadata)
}

func TestClusterConfigPatchDataServerRejectsInternalAddressForUnnamedServer(t *testing.T) {
	internalAddress := "internal-2"

	config := ClusterConfig{
		Servers: []Server{
			{Public: "public-1", Internal: "internal-1"},
		},
	}

	_, err := config.PatchDataServer("internal-1", DataServerPatch{
		InternalAddress: &internalAddress,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "cannot patch internal address for unnamed data server")
}

func TestClusterConfigPatchDataServerRejectsEmptyPatch(t *testing.T) {
	name := "server-1"

	config := ClusterConfig{
		Servers: []Server{
			{Name: &name, Public: "public-1", Internal: "internal-1"},
		},
	}

	_, err := config.PatchDataServer(name, DataServerPatch{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "at least one data server patch field must be provided")
}
