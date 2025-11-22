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

package policy

type AntiAffinityMode string

const (
	// Strict Enforce anti-affinity rules strictly. Ensemble choosing fails if rules can't be met.
	// This mode is for high-availability systems needing strict resource separation.
	Strict AntiAffinityMode = "Strict"

	// Relaxed Try to follow anti-affinity rules, but proceed with ensemble choosing if not feasible.
	// Ideal for environments where resource utilization is limited.
	Relaxed AntiAffinityMode = "Relaxed"
)

// AntiAffinity defines rules to prevent co-location of resource based on specified labels and operation mode.
// Labels specifies a set of key-value labels that form the basis of the anti-affinity constraints.
// Mode determines the mode of anti-affinity enforcement, for instance, strict or relaxed.
type AntiAffinity struct {

	// Labels defines a list of label keys used to evaluate anti-affinity constraints for resource placement decisions.
	Labels []string `json:"labels,omitempty" yaml:"labels,omitempty"`

	// Mode specifies the enforcement level of anti-affinity constraints, such as Strict or Relaxed behavior.
	Mode AntiAffinityMode `json:"mode,omitempty" yaml:"mode,omitempty"`
}
