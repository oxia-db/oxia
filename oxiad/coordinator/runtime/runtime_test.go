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

// statusOnlyDataServerController is a Controller stub that only
// implements Status; countReadyDataServers touches nothing else.
type statusOnlyDataServerController struct {
	dataservercontroller.Controller
	status dataservercontroller.Status
}

func (s statusOnlyDataServerController) Status() dataservercontroller.Status {
	return s.status
}

func TestCountReadyDataServers(t *testing.T) {
	running := &proto.DataServerIdentity{Internal: "running", Public: "running"}
	notRunning := &proto.DataServerIdentity{Internal: "not-running", Public: "not-running"}
	draining := &proto.DataServerIdentity{Internal: "draining", Public: "draining"}
	unknown := &proto.DataServerIdentity{Internal: "unknown", Public: "unknown"}

	c := &runtime{
		dataServerControllers: map[string]dataservercontroller.Controller{
			running.GetNameOrDefault():    statusOnlyDataServerController{status: dataservercontroller.Running},
			notRunning.GetNameOrDefault(): statusOnlyDataServerController{status: dataservercontroller.NotRunning},
		},
		drainingNodes: map[string]dataservercontroller.Controller{
			draining.GetNameOrDefault(): statusOnlyDataServerController{status: dataservercontroller.Draining},
		},
	}

	// Running counts; NotRunning and unknown nodes do not.
	assert.Equal(t, 1, c.countReadyDataServers([]*proto.DataServerIdentity{running, notRunning, unknown}))

	// A draining node already completed the handshake, so it counts as ready.
	// This keeps an RF=1 shard whose only ensemble member was removed from the
	// config (and moved to drainingNodes) from stalling the election gate.
	assert.Equal(t, 1, c.countReadyDataServers([]*proto.DataServerIdentity{draining}))

	// Mixed ensemble: running + draining are ready, not-running is not.
	assert.Equal(t, 2, c.countReadyDataServers([]*proto.DataServerIdentity{running, draining, notRunning}))
}
