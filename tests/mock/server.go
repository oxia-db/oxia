// Copyright 2025 StreamNative, Inc.
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

package mock

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/server"
)

func NewServer(t *testing.T, name string) (s *server.Server, addr model.Server) {
	t.Helper()

	var err error
	s, err = server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})

	assert.NoError(t, err)

	tmp := &name
	addr = model.Server{
		Name:     tmp,
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func NewServerWithAddress(t *testing.T, name string, publicAddress string, internalAddress string) (s *server.Server, addr model.Server) {
	t.Helper()

	var err error
	s, err = server.New(server.Config{
		PublicServiceAddr:          publicAddress,
		InternalServiceAddr:        internalAddress,
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})

	assert.NoError(t, err)

	tmp := &name
	addr = model.Server{
		Name:     tmp,
		Public:   publicAddress,
		Internal: internalAddress,
	}

	return s, addr
}
