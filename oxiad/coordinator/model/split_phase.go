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

type SplitPhase uint16

const (
	SplitPhaseInit      SplitPhase = iota
	SplitPhaseBootstrap            // Sending snapshots to children
	SplitPhaseCatchUp              // Children following parent WAL
	SplitPhaseCutover              // Fencing parent, electing children
	SplitPhaseCleanup              // Deleting parent shard
)

func (s SplitPhase) String() string {
	return splitPhaseToString[s]
}

var splitPhaseToString = map[SplitPhase]string{
	SplitPhaseInit:      "Init",
	SplitPhaseBootstrap: "Bootstrap",
	SplitPhaseCatchUp:   "CatchUp",
	SplitPhaseCutover:   "Cutover",
	SplitPhaseCleanup:   "Cleanup",
}

var stringToSplitPhase = map[string]SplitPhase{
	"Init":      SplitPhaseInit,
	"Bootstrap": SplitPhaseBootstrap,
	"CatchUp":   SplitPhaseCatchUp,
	"Cutover":   SplitPhaseCutover,
	"Cleanup":   SplitPhaseCleanup,
}

// MarshalJSON marshals the enum as a quoted json string.
func (s SplitPhase) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	if _, err := buffer.WriteString(splitPhaseToString[s]); err != nil {
		panic(err)
	}
	if _, err := buffer.WriteString(`"`); err != nil {
		panic(err)
	}
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmarshals a quoted json string to the enum value.
func (s *SplitPhase) UnmarshalJSON(b []byte) error {
	var j string
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*s = stringToSplitPhase[j]
	return nil
}
