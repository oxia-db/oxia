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
	"maps"
	"slices"
	"strings"
)

// AnnotationConfirmedDrainedDataServers lists the data servers whose drain
// the user confirms: see ClusterConfiguration.Annotations.
const AnnotationConfirmedDrainedDataServers = "confirmed-drained-data-servers"

// GetConfirmedDrainedDataServers returns the names of the
// confirmed-drained-data-servers annotation, without the blanks around them
// and the empty ones.
func (cc *ClusterConfiguration) GetConfirmedDrainedDataServers() []string {
	var names []string
	for name := range strings.SplitSeq(cc.GetAnnotations()[AnnotationConfirmedDrainedDataServers], ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// ListUnknownAnnotations returns the keys of the annotations that Oxia
// doesn't know, sorted.
func (cc *ClusterConfiguration) ListUnknownAnnotations() []string {
	keys := slices.Sorted(maps.Keys(cc.GetAnnotations()))
	return slices.DeleteFunc(keys, func(key string) bool { return key == AnnotationConfirmedDrainedDataServers })
}
