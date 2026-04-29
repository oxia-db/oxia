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

package assignment

import (
	"testing"

	"github.com/stretchr/testify/assert"

	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
)

func TestValidateAuthorityAddress(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid host:port",
			addr:    "localhost:6649",
			wantErr: false,
		},
		{
			name:    "valid hostname:port",
			addr:    "server1.example.com:6649",
			wantErr: false,
		},
		{
			name:    "valid IP:port",
			addr:    "192.168.1.1:6649",
			wantErr: false,
		},
		{
			name:    "scheme injection http",
			addr:    "http://evil:6649",
			wantErr: true,
			errMsg:  "must not contain a scheme",
		},
		{
			name:    "scheme injection https",
			addr:    "https://evil:6649",
			wantErr: true,
			errMsg:  "must not contain a scheme",
		},
		{
			name:    "path injection",
			addr:    "host:6649/path",
			wantErr: true,
			errMsg:  "must not contain a path",
		},
		{
			name:    "query injection",
			addr:    "host:6649?param=value",
			wantErr: true,
			errMsg:  "must not contain a query string",
		},
		{
			name:    "fragment injection",
			addr:    "host:6649#fragment",
			wantErr: true,
			errMsg:  "must not contain a fragment",
		},
		{
			name:    "empty host",
			addr:    ":6649",
			wantErr: true,
			errMsg:  "has an empty host",
		},
		{
			name:    "missing port",
			addr:    "host",
			wantErr: true,
			errMsg:  "not a valid host:port pair",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rpc2.ValidateAuthorityAddress(tt.addr)
			if tt.wantErr {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
