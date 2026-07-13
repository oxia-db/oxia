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

package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/dataserver"
)

func TestFindDataServerFeaturesIncludesUnknownNodes(t *testing.T) {
	c := &runtime{
		dataServerControllers: map[string]dataservercontroller.Controller{},
		drainingNodes:         map[string]dataservercontroller.Controller{},
	}

	dataServer := &proto.DataServerIdentity{Internal: "s1:6649"}

	features := c.findDataServerFeatures([]*proto.DataServerIdentity{dataServer})

	assert.Contains(t, features, dataServer.GetNameOrDefault())
	assert.Empty(t, features[dataServer.GetNameOrDefault()])
}
