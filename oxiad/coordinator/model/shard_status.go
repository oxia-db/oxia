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

package model

import (
	"bytes"
	"encoding/json"
)

type ShardStatus uint16

const (
	ShardStatusUnknown ShardStatus = iota
	ShardStatusSteadyState
	ShardStatusElection
	ShardStatusDeleting
)

func (s ShardStatus) String() string {
	return toString[s]
}

var toString = map[ShardStatus]string{
	ShardStatusUnknown:     "Unknown",
	ShardStatusSteadyState: "SteadyState",
	ShardStatusElection:    "Election",
	ShardStatusDeleting:    "Deleting",
}

var toShardStatus = map[string]ShardStatus{
	"Unknown":     ShardStatusUnknown,
	"SteadyState": ShardStatusSteadyState,
	"Election":    ShardStatusElection,
	"Deleting":    ShardStatusDeleting,
}

// MarshalJSON marshals the enum as a quoted json string.
func (s ShardStatus) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	if _, err := buffer.WriteString(toString[s]); err != nil {
		panic(err)
	}
	if _, err := buffer.WriteString(`"`); err != nil {
		panic(err)
	}
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmarshals a quoted json string to the enum value.
func (s *ShardStatus) UnmarshalJSON(b []byte) error {
	var j string
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	// If the string cannot be found then it will be set to the Unknown status value.
	*s = toShardStatus[j]
	return nil
}
