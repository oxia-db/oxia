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

package option

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestK8sMetadataUnmarshalJSON(t *testing.T) {
	km := K8sMetadata{}
	require.NoError(t, json.Unmarshal([]byte(`{"namespace":"oxia","configMapName":"status","configName":"config"}`), &km))
	require.Equal(t, K8sMetadata{Namespace: "oxia", StatusName: "status", ConfigName: "config"}, km)

	require.Error(t, json.Unmarshal([]byte(`{"namespace":1}`), &km))
	require.ErrorContains(t, json.Unmarshal([]byte(`{"statusName":"a","configMapName":"b"}`), &km),
		"kubernetes.statusName and deprecated kubernetes.configMapName")
}

func TestK8sMetadataUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	km := K8sMetadata{}
	require.Error(t, yaml.Unmarshal([]byte("[a, b]"), &km))
}

func TestK8sMetadataNameOrDefault(t *testing.T) {
	km := K8sMetadata{}
	require.Equal(t, DefaultK8sStatusName, km.StatusNameOrDefault())
	require.Equal(t, DefaultK8sConfigName, km.ConfigNameOrDefault())
}
