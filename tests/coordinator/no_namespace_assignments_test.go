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

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/proto"
	clientrpc "github.com/oxia-db/oxia/common/rpc"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

func TestCoordinator_NoNamespacesSendsEmptyAssignments(t *testing.T) {
	s1, sa1 := newServer(t)
	defer s1.Close()

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	clusterConfig := newClusterConfig([]*proto.Namespace{}, []*proto.DataServerIdentity{sa1})
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))
	defer coordinatorInstance.Close()

	clientPool := clientrpc.NewClientPool(nil, nil)
	defer clientPool.Close()
	healthClient, err := clientPool.GetHealthRpc(sa1.Public)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{
			Service: oxiadcommonrpc.ReadinessProbeService,
		})
		return err == nil && response.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
	}, 30*time.Second, 10*time.Millisecond)
}
