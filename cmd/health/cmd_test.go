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

package health

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

func TestHealthCmd(t *testing.T) {
	_health := health.NewServer()
	server, err := rpc.Default.StartGrpcServer("health", "localhost:0", func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, _health)
	}, nil, &auth.Options{})
	assert.NoError(t, err)
	defer func() {
		_ = server.Close()
	}()

	_health.SetServingStatus("serving", grpc_health_v1.HealthCheckResponse_SERVING)
	_health.SetServingStatus("not-serving", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	_health.SetServingStatus("unknown", grpc_health_v1.HealthCheckResponse_UNKNOWN)

	portArg := fmt.Sprintf("--port=%d", server.Port())
	hostArg := "--host=localhost"

	for _, test := range []struct {
		name         string
		args         []string
		expectedCode codes.Code
	}{
		{"happy path", []string{"health", hostArg, portArg}, codes.OK},
		{"incorrect port", []string{"health", hostArg, "--port=1"}, codes.Unavailable},
		{"serving", []string{"health", hostArg, portArg, "--service=serving"}, codes.OK},
		{"not-serving", []string{"health", hostArg, portArg, "--service=not-serving"}, codes.Unknown},
		{"unknown", []string{"health", hostArg, portArg, "--service=unknown"}, codes.Unknown},
		{"invalid", []string{"health", hostArg, portArg, "--service=invalid"}, codes.NotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			config = NewConfig()

			Cmd.SetArgs(test.args)
			err := Cmd.Execute()

			assert.Equal(t, test.expectedCode, status.Code(err))
		})
	}
}
