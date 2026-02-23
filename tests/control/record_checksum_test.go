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

package control

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	clientrpc "github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	metric "github.com/oxia-db/oxia/oxiad/common/metric"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestControlRequestRecordChecksum(t *testing.T) {
	// Start a Prometheus metrics endpoint to scrape from.
	// All dataservers in this process share the global OTel MeterProvider,
	// so metrics recorded by leader/follower controllers are visible here.
	metricsServer, err := metric.Start("localhost:0")
	assert.NoError(t, err)
	defer metricsServer.Close()
	metricsURL := fmt.Sprintf("http://localhost:%d/metrics", metricsServer.Port())

	checksumInterval := 5 * time.Second
	s1, sa1 := mock.NewServerWithOptions(t, "s1", func(o *option.Options) {
		o.Scheduler.Checksum.Interval = commonoption.Duration(checksumInterval)
	})
	s2, sa2 := mock.NewServerWithOptions(t, "s2", func(o *option.Options) {
		o.Scheduler.Checksum.Interval = commonoption.Duration(checksumInterval)
	})
	s3, sa3 := mock.NewServerWithOptions(t, "s3", func(o *option.Options) {
		o.Scheduler.Checksum.Interval = commonoption.Duration(checksumInterval)
	})
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	serverInstanceIndex := map[string]*dataserver.Server{
		sa1.GetIdentifier(): s1,
		sa2.GetIdentifier(): s2,
		sa3.GetIdentifier(): s3,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc.NewRpcProvider(clientrpc.NewClientPool(nil, nil)))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("default"))
	assert.NoError(t, err)
	defer client.Close()

	// Write initial data
	_, _, err = client.Put(context.Background(), "/key1", []byte("value1"))
	assert.NoError(t, err)

	// Wait for the checksum feature to be enabled on all replicas
	resource := coordinatorInstance.StatusResource().Load()
	shardMetadata := resource.Namespaces["default"].Shards[0]
	leader := shardMetadata.Leader
	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetIdentifier()
		if targetId == leader.GetIdentifier() {
			assert.Eventually(t, func() bool {
				lead, err := serverInstanceIndex[targetId].GetShardDirector().GetLeader(0)
				return err == nil && lead.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM)
			}, 10*time.Second, 100*time.Millisecond)
		} else {
			assert.Eventually(t, func() bool {
				follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
				return err == nil && follow.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM)
			}, 10*time.Second, 100*time.Millisecond)
		}
	}

	// Write more data after feature is enabled so checksums are computed
	_, _, err = client.Put(context.Background(), "/key2", []byte("value2"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "/key3", []byte("value3"))
	assert.NoError(t, err)

	// Trigger checksum recording on the leader
	lead, err := serverInstanceIndex[leader.GetIdentifier()].GetShardDirector().GetLeader(0)
	assert.NoError(t, err)
	lead.ProposeRecordChecksum(context.Background())

	// Verify the checksum gauge metric appears in the Prometheus endpoint
	// with the expected labels and a non-zero value
	assert.Eventually(t, func() bool {
		body := fetchMetrics(t, metricsURL)
		if body == "" {
			return false
		}
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "#") || !strings.Contains(line, "oxia_dataserver_db_checksum") {
				continue
			}
			if strings.Contains(line, `shard="0"`) &&
				strings.Contains(line, `oxia_namespace="default"`) &&
				strings.Contains(line, "commit_offset=") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					return parts[len(parts)-1] != "0"
				}
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond)
}

func fetchMetrics(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(body)
}
