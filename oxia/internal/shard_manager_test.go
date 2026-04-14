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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{hashRange(1, 2), hashRange(3, 6), false},
		{hashRange(1, 4), hashRange(3, 6), true},
		{hashRange(4, 5), hashRange(3, 6), true},
		{hashRange(5, 8), hashRange(3, 6), true},
		{hashRange(7, 8), hashRange(3, 6), false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}

func TestValidateClusterID(t *testing.T) {
	clusterID, err := validateClusterID("", "cluster-a")
	require.NoError(t, err)
	assert.Equal(t, "cluster-a", clusterID)

	clusterID, err = validateClusterID("cluster-a", "cluster-a")
	require.NoError(t, err)
	assert.Equal(t, "cluster-a", clusterID)

	clusterID, err = validateClusterID("cluster-a", "")
	require.NoError(t, err)
	assert.Equal(t, "cluster-a", clusterID)

	_, err = validateClusterID("cluster-a", "cluster-b")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrClusterIDMismatch)
}
