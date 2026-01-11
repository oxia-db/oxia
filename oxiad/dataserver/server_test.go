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

package dataserver

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
)

func TestNewServer(t *testing.T) {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.BindAddress = "localhost:0"
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()

	server, err := New(options)
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", server.metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))
}

func TestNewServerClosableWithHealthWatch(t *testing.T) {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.BindAddress = "localhost:0"
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()

	server, err := New(options)
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(nil, nil)

	client, closer, err := clientPool.GetHealthRpc(fmt.Sprintf("127.0.0.1:%v", server.InternalPort()))
	assert.NoError(t, err)
	defer closer.Close()
	watchStream, err := client.Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{Service: ""})
	assert.NoError(t, err)

	err = server.Close()
	assert.NoError(t, err)

	assert.NoError(t, watchStream.CloseSend())
}
