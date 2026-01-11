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

package mock

import (
	"fmt"
	"testing"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func NewServer(t *testing.T, name string) (s *dataserver.Server, addr model.Server) {
	t.Helper()
	dataServerOption := option.NewDefaultOptions()
	dataServerOption.Server.Public.BindAddress = "localhost:0"
	dataServerOption.Server.Internal.BindAddress = "localhost:0"
	dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
	dataServerOption.Storage.Database.Dir = t.TempDir()
	dataServerOption.Storage.WAL.Dir = t.TempDir()
	var err error
	s, err = dataserver.New(dataServerOption)

	assert.NoError(t, err)

	tmp := &name
	addr = model.Server{
		Name:     tmp,
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func NewServerWithAddress(t *testing.T, name string, publicAddress string, internalAddress string) (s *dataserver.Server, addr model.Server) {
	t.Helper()
	dataServerOption := option.NewDefaultOptions()
	dataServerOption.Server.Public.BindAddress = "localhost:0"
	dataServerOption.Server.Internal.BindAddress = "localhost:0"
	dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
	dataServerOption.Storage.Database.Dir = t.TempDir()
	dataServerOption.Storage.WAL.Dir = t.TempDir()

	var err error
	s, err = dataserver.New(dataServerOption)

	assert.NoError(t, err)

	tmp := &name
	addr = model.Server{
		Name:     tmp,
		Public:   publicAddress,
		Internal: internalAddress,
	}

	return s, addr
}
