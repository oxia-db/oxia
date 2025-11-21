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

package coordinator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCoordinator_MarshalingError(t *testing.T) {
	config := Config{
		InternalServiceAddr:       "123",
		AdminServiceAddr:          "123",
		InternalSecureServiceAddr: "123",
		MetadataProviderName:      "123",
		K8SMetadataConfigMapName:  "123",
	}
	_, err := json.Marshal(config)
	assert.Nil(t, err)
}
