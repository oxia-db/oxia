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

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/common/metric"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestControlRequestRecordChecksum(t *testing.T) {
	// Start a Prometheus metrics endpoint to scrape from.
	// All dataservers in this process share the global OTel MeterProvider,
	// so metrics recorded by leader/follower controllers are visible here.
	metricsServer, err := metric.Start("localhost:0", nil)
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
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(provider.ClusterStatusCodec)
	clusterConfig := newDefaultClusterConfig(sa1, sa2, sa3)
	configProvider := memory.NewProvider(provider.ClusterConfigCodec)
	_, err = configProvider.Store(clusterConfig, provider.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(
		t,
		metadataProvider,
		configProvider,
		rpc.NewRpcProviderFactory(nil),
	)
	defer coordinatorInstance.Close()

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("default"))
	assert.NoError(t, err)
	defer client.Close()

	// Write initial data
	_, _, err = client.Put(context.Background(), "/key1", []byte("value1"))
	assert.NoError(t, err)

	// Wait for the checksum feature to be enabled on all replicas
	resource := coordinatorInstance.Metadata().GetStatus()
	shardMetadata := resource.Namespaces["default"].Shards[0]
	leader := shardMetadata.Leader
	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetNameOrDefault()
		if targetId == leader.GetNameOrDefault() {
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
	lead, err := serverInstanceIndex[leader.GetNameOrDefault()].GetShardDirector().GetLeader(0)
	assert.NoError(t, err)
	lead.ProposeRecordChecksum(context.Background())

	// Verify the DB checksum gauge metric appears in the Prometheus endpoint
	// as a shard-scoped series with a non-zero value.
	var firstDbMetric checksumMetric
	assert.Eventually(t, func() bool {
		var found bool
		firstDbMetric, found = parseChecksumMetric(t, metricsURL, "oxia_dataserver_db_checksum")
		return found
	}, 30*time.Second, 200*time.Millisecond)
	assert.NotContains(t, firstDbMetric.labels, "commit_offset")
	assert.NotEqual(t, "0", firstDbMetric.value)

	// Verify the WAL checksum gauge also appears
	var firstWalMetric checksumMetric
	assert.Eventually(t, func() bool {
		var found bool
		firstWalMetric, found = parseChecksumMetric(t, metricsURL, "oxia_dataserver_wal_checksum")
		return found
	}, 30*time.Second, 200*time.Millisecond)
	assert.NotContains(t, firstWalMetric.labels, "commit_offset")
	assert.NotEqual(t, "0", firstWalMetric.value)

	// Write more data and record the checksum again
	_, _, err = client.Put(context.Background(), "/key4", []byte("value4"))
	assert.NoError(t, err)

	lead.ProposeRecordChecksum(context.Background())

	// Verify the DB checksum series updates in place with a different value.
	assert.Eventually(t, func() bool {
		metric, found := parseChecksumMetric(t, metricsURL, "oxia_dataserver_db_checksum")
		return found && metric.labels == firstDbMetric.labels && metric.value != firstDbMetric.value
	}, 30*time.Second, 200*time.Millisecond)

	// Verify the WAL checksum series also updates in place with a different value.
	assert.Eventually(t, func() bool {
		metric, found := parseChecksumMetric(t, metricsURL, "oxia_dataserver_wal_checksum")
		return found && metric.labels == firstWalMetric.labels && metric.value != firstWalMetric.value
	}, 30*time.Second, 200*time.Millisecond)
}

type checksumMetric struct {
	labels string
	value  string
}

// parseChecksumMetric scrapes the Prometheus endpoint and returns the shard-scoped
// checksum series for the given metricName with shard=0 and namespace=default.
func parseChecksumMetric(t *testing.T, url string, metricName string) (checksumMetric, bool) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return checksumMetric{}, false
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return checksumMetric{}, false
	}

	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "#") || !strings.Contains(line, metricName) {
			continue
		}
		if !strings.Contains(line, `shard="0"`) || !strings.Contains(line, `oxia_namespace="default"`) {
			continue
		}
		if strings.Contains(line, `commit_offset="`) {
			continue
		}

		labelStart := strings.Index(line, "{")
		labelEnd := strings.Index(line, "}")
		if labelStart < 0 || labelEnd < 0 || labelEnd <= labelStart {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		if value := parts[len(parts)-1]; value != "0" {
			return checksumMetric{
				labels: line[labelStart+1 : labelEnd],
				value:  value,
			}, true
		}
	}
	return checksumMetric{}, false
}
