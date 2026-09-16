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

package database

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSecondaryIndexKey(t *testing.T) {
	primaryKey := "/primary\x01key"
	tests := []struct {
		name              string
		indexKey          string
		expectedPrimary   string
		expectedSecondary string
	}{
		{
			name:              "standard",
			indexKey:          idxKeyPrefix + "/index/secondary" + idxSeparator + url.PathEscape(primaryKey),
			expectedPrimary:   primaryKey,
			expectedSecondary: "secondary",
		},
		{
			name:              "separator in secondary key",
			indexKey:          idxKeyPrefix + "/index/first" + idxSeparator + "second" + idxSeparator + url.PathEscape(primaryKey),
			expectedPrimary:   primaryKey,
			expectedSecondary: "first" + idxSeparator + "second",
		},
		{
			name:              "empty secondary key",
			indexKey:          idxKeyPrefix + "/index/" + idxSeparator + url.PathEscape(primaryKey),
			expectedPrimary:   primaryKey,
			expectedSecondary: "",
		},
		{
			name:              "newline in secondary key",
			indexKey:          idxKeyPrefix + "/index/first\nsecond" + idxSeparator + url.PathEscape(primaryKey),
			expectedPrimary:   primaryKey,
			expectedSecondary: "first\nsecond",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualPrimary, actualSecondary, err := ParseSecondaryIndexKey(tc.indexKey)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedPrimary, actualPrimary)
			assert.Equal(t, tc.expectedSecondary, actualSecondary)
		})
	}
}

func TestParseSecondaryIndexKeyRejectsMalformedKey(t *testing.T) {
	_, _, err := ParseSecondaryIndexKey(idxKeyPrefix + "/index/secondary")
	assert.ErrorIs(t, err, ErrInvalidSecondaryIndexKey)

	_, _, err = ParseSecondaryIndexKey(idxKeyPrefix + "/index/secondary" + idxSeparator + "%zz")
	assert.Error(t, err)
}
