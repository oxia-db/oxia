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

	"github.com/stretchr/testify/assert"
)

func TestClusterConfigurationGetConfirmedDrainedDataServers(t *testing.T) {
	var nilConfig *ClusterConfiguration
	assert.Empty(t, nilConfig.GetConfirmedDrainedDataServers())
	assert.Empty(t, (&ClusterConfiguration{}).GetConfirmedDrainedDataServers())

	config := &ClusterConfiguration{Annotations: map[string]string{
		AnnotationConfirmedDrainedDataServers: " ds-4, ,ds-5 ,",
	}}
	assert.Equal(t, []string{"ds-4", "ds-5"}, config.GetConfirmedDrainedDataServers())
}

func TestClusterConfigurationListUnknownAnnotations(t *testing.T) {
	var nilConfig *ClusterConfiguration
	assert.Empty(t, nilConfig.ListUnknownAnnotations())

	config := &ClusterConfiguration{Annotations: map[string]string{
		AnnotationConfirmedDrainedDataServers: "ds-4",
		"zeta":                                "",
		"confirmed-drained":                   "ds-5",
	}}
	assert.Equal(t, []string{"confirmed-drained", "zeta"}, config.ListUnknownAnnotations())
}
