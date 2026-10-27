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

package auth

import (
	"context"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
)

func TestOptionsIsEnabled(t *testing.T) {
	enabled := true
	disabled := false
	tests := []struct {
		name     string
		options  Options
		expected bool
	}{
		{"zero value", Options{}, false},
		{"provider set", Options{Provider: ProviderOIDC}, true},
		{"provider set overrides disabled", Options{Provider: ProviderOIDC, Enabled: &disabled}, true},
		{"enabled without provider", Options{Enabled: &enabled}, true},
		{"disabled without provider", Options{Enabled: &disabled}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.options.IsEnabled())
		})
	}
}

func TestNewAuthenticationProviderRejectsUnsupportedProvider(t *testing.T) {
	provider, err := NewAuthenticationProvider(context.Background(), Options{Provider: "ldap"})

	assert.ErrorIs(t, err, errs.ErrUnsupportedProvider)
	assert.Nil(t, provider)
}
