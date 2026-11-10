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

package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBackupDuration(t *testing.T) {
	for value, want := range map[string]time.Duration{
		"7d":    7 * 24 * time.Hour,
		"1d":    24 * time.Hour,
		"90m":   90 * time.Minute,
		"1h30m": 90 * time.Minute,
		"30s":   30 * time.Second,
	} {
		d, err := ParseBackupDuration(value)
		require.NoError(t, err, value)
		assert.Equal(t, want, d, value)
	}
	for _, value := range []string{"", "d", "7dd", "-1d", "1.5d", "7 d", "week"} {
		_, err := ParseBackupDuration(value)
		assert.Error(t, err, value)
	}
}

func TestClusterConfigurationGetBackupWithDefaults(t *testing.T) {
	t.Run("nil configuration", func(t *testing.T) {
		b := (*ClusterConfiguration)(nil).GetBackupWithDefaults()
		assert.False(t, b.GetEnabled())
		assert.Equal(t, time.Hour, b.GetIntervalDurationOrDefault())
		assert.Equal(t, 7*24*time.Hour, b.GetRetentionDurationOrDefault())
		assert.Equal(t, uint32(2), b.GetMaxConcurrentPerServerOrDefault())
		assert.Equal(t, 30*time.Second, b.GetScheduleIntervalDurationOrDefault())
		assert.Equal(t, 24*time.Hour, b.GetCleanupIntervalDurationOrDefault())
	})
	t.Run("explicit values", func(t *testing.T) {
		cc := &ClusterConfiguration{Backup: &BackupConfig{
			Enabled:                true,
			Destination:            "s3://bucket/prefix",
			Interval:               "15m",
			Retention:              "2d",
			MaxConcurrentPerServer: 4,
			ScheduleInterval:       "5s",
			CleanupInterval:        "6h",
		}}
		b := cc.GetBackupWithDefaults()
		assert.True(t, b.GetEnabled())
		assert.Equal(t, "s3://bucket/prefix", b.GetDestination())
		assert.Equal(t, 15*time.Minute, b.GetIntervalDurationOrDefault())
		assert.Equal(t, 48*time.Hour, b.GetRetentionDurationOrDefault())
		assert.Equal(t, uint32(4), b.GetMaxConcurrentPerServerOrDefault())
		assert.Equal(t, uint32(2), (&BackupConfig{}).GetMaxConcurrentPerServerOrDefault(), "zero takes the default")
		assert.Equal(t, 5*time.Second, b.GetScheduleIntervalDurationOrDefault())
		assert.Equal(t, 6*time.Hour, b.GetCleanupIntervalDurationOrDefault())
	})
	t.Run("invalid values fall back to the defaults", func(t *testing.T) {
		b := &BackupConfig{Interval: "soon", Retention: "-1h"}
		assert.Equal(t, time.Hour, b.GetIntervalDurationOrDefault())
		assert.Equal(t, 7*24*time.Hour, b.GetRetentionDurationOrDefault())
	})
}

func TestBackupConfigValidate(t *testing.T) {
	require.NoError(t, (*BackupConfig)(nil).Validate())
	require.NoError(t, (&BackupConfig{}).Validate(), "disabled with the defaults")
	require.NoError(t, (&BackupConfig{Enabled: true, Destination: "s3://b/p", Interval: "1h", Retention: "1d"}).Validate())
	for name, config := range map[string]*BackupConfig{
		"enabled without destination":        {Enabled: true},
		"invalid interval":                   {Interval: "1 hour"},
		"zero interval":                      {Interval: "0s"},
		"invalid retention":                  {Retention: "forever"},
		"retention not longer than interval": {Interval: "2h", Retention: "2h"},
		"invalid schedule interval":          {ScheduleInterval: "fast"},
		"negative cleanup interval":          {CleanupInterval: "-24h"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.ErrorContains(t, config.Validate(), "backup.")
		})
	}
}

func TestClusterConfigurationValidateChecksBackup(t *testing.T) {
	cc := &ClusterConfiguration{Backup: &BackupConfig{Enabled: true}}
	assert.ErrorContains(t, cc.Validate(), "backup.destination")
}
