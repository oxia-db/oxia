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

package backup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanPicksTheNewestBackupAtOrBeforeAsOf(t *testing.T) {
	backups := []Summary{summary(1, FullRange, 1), summary(1, FullRange, 10), summary(1, FullRange, 20)}
	parts, err := Plan(backups, retentionNow().Add(-5*time.Hour))
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, FullRange, parts[0].Range)
	assert.Equal(t, summary(1, FullRange, 10).ID, parts[0].Source.ID)

	exact, err := Plan(backups, summary(1, FullRange, 10).ID.Time)
	require.NoError(t, err)
	assert.Equal(t, summary(1, FullRange, 10).ID, exact[0].Source.ID, "a backup at asOf is included")
}

func TestPlanFailsWhenNoBackupIsOldEnough(t *testing.T) {
	backups := []Summary{summary(1, FullRange, 1)}
	_, err := Plan(backups, retentionNow().Add(-5*time.Hour))
	assert.ErrorIs(t, err, ErrNoBackup)
	_, err = Plan(nil, retentionNow())
	assert.ErrorIs(t, err, ErrNoBackup)
}

func TestPlanFailsWhenARangeIsUncovered(t *testing.T) {
	backups := []Summary{summary(2, leftHalf, 1)}
	_, err := Plan(backups, retentionNow())
	assert.ErrorIs(t, err, ErrNoBackup)
	assert.ErrorContains(t, err, rightHalf.String())
}

func TestPlanRestoresTheParentBeforeASplit(t *testing.T) {
	parent := summary(1, FullRange, 10)
	left := summary(2, leftHalf, 5)
	right := summary(3, rightHalf, 4)
	parts, err := Plan([]Summary{parent, left, right}, retentionNow().Add(-7*time.Hour))
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, parent.ID, parts[0].Source.ID)
	assert.Equal(t, FullRange, parts[0].Range)
}

func TestPlanRestoresTheChildrenAfterASplit(t *testing.T) {
	parent := summary(1, FullRange, 10)
	left := summary(2, leftHalf, 5)
	right := summary(3, rightHalf, 4)
	parts, err := Plan([]Summary{parent, left, right}, retentionNow())
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, Part{Range: leftHalf, Source: left}, parts[0])
	assert.Equal(t, Part{Range: rightHalf, Source: right}, parts[1])
}

func TestPlanRestoresTheOtherHalfFromTheParentWhenOnlyOneChildHasABackup(t *testing.T) {
	parent := summary(1, FullRange, 10)
	left := summary(2, leftHalf, 5)
	parts, err := Plan([]Summary{parent, left}, retentionNow())
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, Part{Range: leftHalf, Source: left}, parts[0])
	assert.Equal(t, Part{Range: rightHalf, Source: parent}, parts[1], "the parent, filtered to the right half")
}

func TestPlanSplitsTheParentAroundANewerMiddleRange(t *testing.T) {
	quarter := uint32(1 << 30)
	middle := summary(4, Range{Min: quarter, Max: 3*quarter - 1}, 5)
	parent := summary(1, FullRange, 10)
	parts, err := Plan([]Summary{parent, middle}, retentionNow())
	require.NoError(t, err)
	require.Len(t, parts, 3)
	assert.Equal(t, Part{Range: Range{Min: 0, Max: quarter - 1}, Source: parent}, parts[0])
	assert.Equal(t, Part{Range: middle.Range, Source: middle}, parts[1])
	assert.Equal(t, Part{Range: Range{Min: 3 * quarter, Max: FullRange.Max}, Source: parent}, parts[2])
}

func TestPlanUsesTheStartTimeOfTheBackupsWithinASecond(t *testing.T) {
	at := retentionNow()
	older := Summary{Namespace: "ns", Shard: 1, ID: NewID(at, "aaaa"), CreatedAt: at.Add(100 * time.Millisecond),
		Range: FullRange}
	newer := Summary{Namespace: "ns", Shard: 1, ID: NewID(at, "bbbb"), CreatedAt: at.Add(600 * time.Millisecond),
		Range: FullRange}
	parts, err := Plan([]Summary{older, newer}, at.Add(300*time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, older.ID, parts[0].Source.ID, "the newer one started after asOf")
	_, err = Plan([]Summary{older, newer}, at.Add(50*time.Millisecond))
	assert.ErrorIs(t, err, ErrNoBackup)
}
