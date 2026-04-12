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

package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactToken(t *testing.T) {
	tests := []struct {
		name     string
		token    string
		expected string
	}{
		{"empty", "", "***"},
		{"short", "abc", "***"},
		{"exactly8", "12345678", "***"},
		{"9chars", "123456789", "***23456789"},
		{"long token", "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature", "***ignature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redactToken(tt.token)
			assert.Equal(t, tt.expected, result)
			// Ensure the original token is never fully present in the result
			if len(tt.token) > 8 {
				assert.NotContains(t, result, tt.token)
			}
		})
	}
}
