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

package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "valid simple name",
			namespace: "default",
			wantErr:   false,
		},
		{
			name:      "valid name with hyphens",
			namespace: "my-namespace",
			wantErr:   false,
		},
		{
			name:      "valid name with underscores",
			namespace: "my_namespace",
			wantErr:   false,
		},
		{
			name:      "valid name with dots",
			namespace: "my.ns",
			wantErr:   false,
		},
		{
			name:      "valid name with numbers",
			namespace: "ns123",
			wantErr:   false,
		},
		{
			name:      "valid single character",
			namespace: "a",
			wantErr:   false,
		},
		{
			name:      "empty namespace",
			namespace: "",
			wantErr:   true,
			errMsg:    "must not be empty",
		},
		{
			name:      "path traversal with dot-dot",
			namespace: "../evil",
			wantErr:   true,
			errMsg:    "path traversal characters",
		},
		{
			name:      "path traversal with dot-dot only",
			namespace: "..",
			wantErr:   true,
			errMsg:    "path traversal characters",
		},
		{
			name:      "forward slash",
			namespace: "ns/evil",
			wantErr:   true,
			errMsg:    "path traversal characters",
		},
		{
			name:      "backslash",
			namespace: "ns\\evil",
			wantErr:   true,
			errMsg:    "path traversal characters",
		},
		{
			name:      "starts with dot",
			namespace: ".hidden",
			wantErr:   true,
			errMsg:    "invalid characters",
		},
		{
			name:      "starts with hyphen",
			namespace: "-invalid",
			wantErr:   true,
			errMsg:    "invalid characters",
		},
		{
			name:      "contains spaces",
			namespace: "my namespace",
			wantErr:   true,
			errMsg:    "invalid characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNamespace(tt.namespace)
			if tt.wantErr {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
