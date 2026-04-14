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
)

func TestClusterConfig(t *testing.T) {
	cc1 := ClusterConfig{
		Namespaces: []NamespaceConfig{{
			Name:              "ns1",
			InitialShardCount: 2,
			ReplicationFactor: 3,
			KeySorting:        KeySortingHierarchical,
		}},
		Servers: []Server{{
			Public:   "f1",
			Internal: "f1",
		}, {
			Public:   "f2",
			Internal: "f2",
		}},
	}

	cc2 := ClusterConfig{
		Namespaces: []NamespaceConfig{{
			Name:              "ns1",
			InitialShardCount: 2,
			ReplicationFactor: 3,
			KeySorting:        KeySortingHierarchical,
		}},
		Servers: []Server{{
			Public:   "f1",
			Internal: "f1",
		}, {
			Public:   "f2",
			Internal: "f2",
		}},
	}

	assert.Equal(t, cc1, cc2)
	assert.NotSame(t, &cc1, &cc2)
}

func validConfig() ClusterConfig {
	return ClusterConfig{
		Namespaces: []NamespaceConfig{{
			Name:              "ns1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: []Server{
			{Public: "s1:6648", Internal: "s1:6649"},
			{Public: "s2:6648", Internal: "s2:6649"},
			{Public: "s3:6648", Internal: "s3:6649"},
		},
		AllowExtraAuthorities: []string{"extra-1:6648"},
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cc := validConfig()
	assert.NoError(t, cc.Validate())
}

func TestValidate_InvalidAllowExtraAuthorities(t *testing.T) {
	cc := validConfig()
	cc.AllowExtraAuthorities = []string{"http://bad:6648"}
	assert.ErrorContains(t, cc.Validate(), `invalid allowExtraAuthorities entry "http://bad:6648"`)
}

func TestValidate_NoServers(t *testing.T) {
	cc := validConfig()
	cc.Servers = nil
	assert.ErrorContains(t, cc.Validate(), "at least one server must be configured")
}

func TestValidate_NoNamespaces(t *testing.T) {
	cc := validConfig()
	cc.Namespaces = nil
	assert.ErrorContains(t, cc.Validate(), "at least one namespace must be configured")
}

func TestValidate_EmptyNamespaceName(t *testing.T) {
	cc := validConfig()
	cc.Namespaces[0].Name = ""
	assert.ErrorContains(t, cc.Validate(), "namespace must not be empty")
}

func TestValidate_ReplicationFactorZero(t *testing.T) {
	cc := validConfig()
	cc.Namespaces[0].ReplicationFactor = 0
	assert.ErrorContains(t, cc.Validate(), "invalid replicationFactor=0, must be >= 1")
}

func TestValidate_InitialShardCountZero(t *testing.T) {
	cc := validConfig()
	cc.Namespaces[0].InitialShardCount = 0
	assert.ErrorContains(t, cc.Validate(), "invalid initialShardCount=0, must be >= 1")
}

func TestValidate_ReplicationFactorExceedsServers(t *testing.T) {
	cc := validConfig()
	cc.Namespaces[0].ReplicationFactor = 5
	assert.ErrorContains(t, cc.Validate(), "replicationFactor=5 but only 3 servers are configured")
}

func TestClusterConfig_Validate_NamespaceNames(t *testing.T) {
	tests := []struct {
		name    string
		nsName  string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid name",
			nsName:  "default",
			wantErr: false,
		},
		{
			name:    "valid name with dots",
			nsName:  "my.namespace",
			wantErr: false,
		},
		{
			name:    "valid name with hyphens and underscores",
			nsName:  "my-ns_01",
			wantErr: false,
		},
		{
			name:    "path traversal dot-dot-slash",
			nsName:  "../evil",
			wantErr: true,
			errMsg:  "path traversal sequence",
		},
		{
			name:    "forward slash",
			nsName:  "ns/evil",
			wantErr: true,
			errMsg:  "path separator",
		},
		{
			name:    "backslash",
			nsName:  "ns\\evil",
			wantErr: true,
			errMsg:  "path separator",
		},
		{
			name:    "dot-dot only",
			nsName:  "..",
			wantErr: true,
			errMsg:  "path traversal sequence",
		},
		{
			name:    "starts with dot",
			nsName:  ".hidden",
			wantErr: true,
			errMsg:  "contains invalid characters",
		},
		{
			name:    "contains spaces",
			nsName:  "my namespace",
			wantErr: true,
			errMsg:  "contains invalid characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := validConfig()
			cc.Namespaces[0].Name = tt.nsName
			err := cc.Validate()
			if tt.wantErr {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
