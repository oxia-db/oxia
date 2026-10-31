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
	"fmt"
	"time"
)

// Validate checks the fields of the policy that are set.
func (p *SchedulerPolicy) Validate() error {
	for name, value := range map[string]string{
		"interval":    p.GetInterval(),
		"moveTimeout": p.GetMoveTimeout(),
		"moveBackoff": p.GetMoveBackoff(),
		"stableTime":  p.GetStableTime(),
	} {
		if value == "" {
			continue
		}
		if duration, err := time.ParseDuration(value); err != nil || duration <= 0 {
			return fmt.Errorf("%s must be a positive duration: %q", name, value)
		}
	}
	if tolerance := p.GetTolerance(); tolerance < 0 || tolerance >= 1 {
		return fmt.Errorf("tolerance must be at least 0 and below 1: %v", tolerance)
	}
	return nil
}
