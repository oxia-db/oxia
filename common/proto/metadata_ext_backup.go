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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This file owns the backup section of the cluster configuration: its
// defaults and its validation.

const (
	defaultBackupInterval                      = 1 * time.Hour
	defaultBackupIntervalString                = "1h"
	defaultBackupRetention                     = 7 * 24 * time.Hour
	defaultBackupRetentionString               = "7d"
	defaultBackupMaxConcurrentPerServer uint32 = 2
	defaultBackupScheduleInterval              = 30 * time.Second
	defaultBackupScheduleIntervalString        = "30s"
	defaultBackupCleanupInterval               = 24 * time.Hour
	defaultBackupCleanupIntervalString         = "24h"
	hoursPerDay                                = 24
)

// ParseBackupDuration parses a duration of the backup configuration: the
// time.ParseDuration syntax, or a whole number of days with a "d" suffix,
// e.g. "7d".
func ParseBackupDuration(value string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(value, "d"); ok {
		if n, err := strconv.ParseUint(days, 10, 32); err == nil {
			return time.Duration(n) * hoursPerDay * time.Hour, nil
		}
	}
	return time.ParseDuration(value)
}

// Validate checks the backup configuration, when there is one. An enabled
// policy needs a destination, and a retention longer than the interval.
func (c *BackupConfig) Validate() error {
	if c == nil {
		return nil
	}
	durations := []struct {
		name  string
		value string
	}{
		{"interval", c.GetInterval()},
		{"retention", c.GetRetention()},
		{"scheduleInterval", c.GetScheduleInterval()},
		{"cleanupInterval", c.GetCleanupInterval()},
	}
	for _, d := range durations {
		if d.value == "" {
			continue
		}
		parsed, err := ParseBackupDuration(d.value)
		if err != nil {
			return fmt.Errorf("cluster configuration: invalid backup.%s: %w", d.name, err)
		}
		if parsed <= 0 {
			return fmt.Errorf("cluster configuration: backup.%s must be positive: %q", d.name, d.value)
		}
	}
	if c.GetRetentionDurationOrDefault() <= c.GetIntervalDurationOrDefault() {
		return fmt.Errorf("cluster configuration: backup.retention %q must be longer than backup.interval %q",
			c.GetRetention(), c.GetInterval())
	}
	if c.GetEnabled() && strings.TrimSpace(c.GetDestination()) == "" {
		return errors.New("cluster configuration: backup.destination must be set when backups are enabled")
	}
	return nil
}

func (c *BackupConfig) GetIntervalDurationOrDefault() time.Duration {
	return backupDurationOrDefault(c.GetInterval(), defaultBackupInterval)
}

func (c *BackupConfig) GetRetentionDurationOrDefault() time.Duration {
	return backupDurationOrDefault(c.GetRetention(), defaultBackupRetention)
}

func (c *BackupConfig) GetScheduleIntervalDurationOrDefault() time.Duration {
	return backupDurationOrDefault(c.GetScheduleInterval(), defaultBackupScheduleInterval)
}

func (c *BackupConfig) GetCleanupIntervalDurationOrDefault() time.Duration {
	return backupDurationOrDefault(c.GetCleanupInterval(), defaultBackupCleanupInterval)
}

func (c *BackupConfig) GetMaxConcurrentPerServerOrDefault() uint32 {
	if c == nil || c.GetMaxConcurrentPerServer() == 0 {
		return defaultBackupMaxConcurrentPerServer
	}
	return c.GetMaxConcurrentPerServer()
}

// backupDurationOrDefault returns the parsed value, or the default when the
// value is empty or invalid: Validate rejects an invalid value, so a caller
// that bypassed it still gets a usable duration.
func backupDurationOrDefault(value string, defaultValue time.Duration) time.Duration {
	if value == "" {
		return defaultValue
	}
	d, err := ParseBackupDuration(value)
	if err != nil || d <= 0 {
		return defaultValue
	}
	return d
}

// GetBackupWithDefaults returns the backup configuration with its defaults
// filled in. Backups are disabled unless the configuration enables them.
func (cc *ClusterConfiguration) GetBackupWithDefaults() *BackupConfig {
	b := &BackupConfig{
		Interval:               defaultBackupIntervalString,
		Retention:              defaultBackupRetentionString,
		MaxConcurrentPerServer: defaultBackupMaxConcurrentPerServer,
		ScheduleInterval:       defaultBackupScheduleIntervalString,
		CleanupInterval:        defaultBackupCleanupIntervalString,
	}
	src := cc.GetBackup()
	if src == nil {
		return b
	}
	b.Enabled = src.GetEnabled()
	b.Destination = src.GetDestination()
	if v := src.GetInterval(); v != "" {
		b.Interval = v
	}
	if v := src.GetRetention(); v != "" {
		b.Retention = v
	}
	if v := src.GetMaxConcurrentPerServer(); v != 0 {
		b.MaxConcurrentPerServer = v
	}
	if v := src.GetScheduleInterval(); v != "" {
		b.ScheduleInterval = v
	}
	if v := src.GetCleanupInterval(); v != "" {
		b.CleanupInterval = v
	}
	return b
}
