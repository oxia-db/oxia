// Copyright 2023 StreamNative, Inc.
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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/streamnative/oxia/common"
)

type Config struct {
	Host    string
	Port    int
	Timeout time.Duration
	Service string
}

func NewConfig() Config {
	return Config{
		Host:    "",
		Port:    common.DefaultInternalPort,
		Timeout: 10 * time.Second,
		Service: "",
	}
}

var (
	Cmd = &cobra.Command{
		Use:   "health",
		Short: "Oxia health probe",
		Long:  `Oxia tool to check the health endpoint of a running service`,
		RunE:  exec,
	}

	config = NewConfig()
)

func init() {
	Cmd.Flags().StringVar(&config.Host, "host", config.Host, "Server host")
	Cmd.Flags().IntVar(&config.Port, "port", config.Port, "Server port")
	Cmd.Flags().DurationVar(&config.Timeout, "timeout", config.Timeout, "Health check timeout")
	Cmd.Flags().StringVar(&config.Service, "service", config.Service, "Health check service")
	Cmd.SilenceUsage = true
	Cmd.SilenceErrors = true
}

func exec(*cobra.Command, []string) error {
	clientPool := common.NewClientPool(nil, nil)

	serverAddress := fmt.Sprintf("%s:%d", config.Host, config.Port)

	rpc, err := clientPool.GetHealthRpc(serverAddress)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	req := &grpc_health_v1.HealthCheckRequest{Service: config.Service}

	resp, err := rpc.Check(ctx, req)
	if err != nil {
		return err
	}

	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return errors.New("unhealthy")
	}

	return nil
}
