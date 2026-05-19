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

package provider

import "encoding/json"

type Identity struct {
	ID            string `json:"id"`
	PublicAddress string `json:"publicAddress,omitempty"`
}

func (i Identity) ElectionValue() string {
	if i.ID == "" && i.PublicAddress == "" {
		return ""
	}
	data, err := json.Marshal(i)
	if err != nil {
		return i.ID
	}
	return string(data)
}

func PublicAddressFromElectionValue(value string) string {
	if value == "" {
		return ""
	}
	var identity Identity
	if err := json.Unmarshal([]byte(value), &identity); err != nil {
		return value
	}
	if identity.PublicAddress != "" {
		return identity.PublicAddress
	}
	return identity.ID
}
