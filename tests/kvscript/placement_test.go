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

package kvscript

import (
	"cmp"
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// placement reads every assigned shard directly, independently of the SDK's
// routing. Comparing those observations with the advertised hash ranges proves
// that a fixture really exercises the placement it claims to cover.
func (r *runner) placement(c command) string {
	c.t.Helper()
	c.checkArgs("keys", "partition", "min-shards", "require-override")
	keys := placementKeys(c)
	minimum := placementMinimum(c, len(keys))
	if c.data.HasArg("require-override") && !c.data.HasArg("partition") {
		c.t.Fatalf("%s: require-override needs a partition argument", c.data.Pos)
	}

	address := r.address
	if r.cluster != nil {
		address = r.cluster.liveAddress()
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("%s: placement connection failed: %v", c.data.Pos, err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			c.t.Errorf("%s: placement connection close failed: %v", c.data.Pos, err)
		}
	}()
	rpc := proto.NewOxiaClientClient(connection)
	assignments := placementAssignments(c, rpc)
	locations := placementLocations(c, assignments, keys)
	used, overridden := checkPlacement(c, assignments, keys, locations)
	if len(used) < minimum {
		c.t.Fatalf("%s: placement uses %d shards, need at least %d; locations=%v",
			c.data.Pos, len(used), minimum, locations)
	}
	if c.data.HasArg("require-override") && !overridden {
		c.t.Fatalf("%s: partition did not override any key's default placement; locations=%v",
			c.data.Pos, locations)
	}
	return "ok\n"
}

func placementKeys(c command) []string {
	c.t.Helper()
	keys := c.args("keys")
	if len(keys) == 0 {
		c.t.Fatalf("%s: placement needs at least one key", c.data.Pos)
	}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			c.t.Fatalf("%s: duplicate placement key %q", c.data.Pos, key)
		}
		seen[key] = true
	}
	return keys
}

func placementMinimum(c command, keyCount int) int {
	c.t.Helper()
	if !c.data.HasArg("min-shards") {
		return 1
	}
	minimum, err := strconv.Atoi(c.arg("min-shards"))
	if err != nil || minimum < 1 || minimum > keyCount {
		c.t.Fatalf("%s: min-shards must be between 1 and the number of placement keys", c.data.Pos)
	}
	return minimum
}

func placementAssignments(c command, rpc proto.OxiaClientClient) []*proto.ShardAssignment {
	c.t.Helper()
	// Take a fresh assignment snapshot for each probe, including after a cluster
	// lifecycle barrier. Cancel the stream before reading the advertised leaders.
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	stream, err := rpc.GetShardAssignments(ctx, &proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace})
	if err != nil {
		c.t.Fatalf("%s: placement assignments request failed: %v", c.data.Pos, err)
	}
	response, err := stream.Recv()
	if err != nil {
		c.t.Fatalf("%s: placement assignments receive failed: %v", c.data.Pos, err)
	}
	namespace := response.GetNamespaces()[constant.DefaultNamespace]
	if namespace.GetShardKeyRouter() != proto.ShardKeyRouter_XXHASH3 || len(namespace.GetAssignments()) == 0 {
		c.t.Fatalf("%s: placement requires nonempty XXHASH3 assignments", c.data.Pos)
	}
	assignments := slices.Clone(namespace.Assignments)
	validatePlacementAssignments(c, assignments)
	return assignments
}

func validatePlacementAssignments(c command, assignments []*proto.ShardAssignment) {
	c.t.Helper()
	seen := make(map[int64]bool, len(assignments))
	for _, assignment := range assignments {
		if assignment == nil || assignment.GetInt32HashRange() == nil || assignment.GetLeader() == "" {
			c.t.Fatalf("%s: placement assignment has no hash range or leader", c.data.Pos)
		}
		if assignment.Shard < 0 || seen[assignment.Shard] {
			c.t.Fatalf("%s: invalid or duplicate placement shard %d", c.data.Pos, assignment.Shard)
		}
		seen[assignment.Shard] = true
	}
	slices.SortFunc(assignments, func(a, b *proto.ShardAssignment) int {
		return cmp.Compare(a.GetInt32HashRange().MinHashInclusive, b.GetInt32HashRange().MinHashInclusive)
	})
	var next uint64
	for _, assignment := range assignments {
		rangeBounds := assignment.GetInt32HashRange()
		if uint64(rangeBounds.MinHashInclusive) != next || rangeBounds.MaxHashInclusive < rangeBounds.MinHashInclusive {
			c.t.Fatalf("%s: placement hash ranges have a gap, overlap, or reversed boundary at shard %d",
				c.data.Pos, assignment.Shard)
		}
		next = uint64(rangeBounds.MaxHashInclusive) + 1
	}
	if next != uint64(math.MaxUint32)+1 {
		c.t.Fatalf("%s: placement hash ranges do not cover uint32 hashes", c.data.Pos)
	}
}

func placementLocations(c command, assignments []*proto.ShardAssignment,
	keys []string) map[string][]int64 {
	c.t.Helper()
	locations := make(map[string][]int64, len(keys))
	for _, assignment := range assignments {
		responses := probePlacementLeader(c, assignment, keys)
		for i, response := range responses {
			if placementPresent(c, response, keys[i], assignment.Shard) {
				locations[keys[i]] = append(locations[keys[i]], assignment.Shard)
			}
		}
	}
	return locations
}

func probePlacementLeader(c command, assignment *proto.ShardAssignment, keys []string) []*proto.GetResponse {
	c.t.Helper()
	connection, err := grpc.NewClient(assignment.Leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(c.t, err, "%s: cannot connect to placement leader", c.data.Pos)
	defer func() { require.NoError(c.t, connection.Close()) }()
	return probePlacementShard(c, proto.NewOxiaClientClient(connection), assignment.Shard, keys)
}

func probePlacementShard(c command, rpc proto.OxiaClientClient, shard int64, keys []string) []*proto.GetResponse {
	c.t.Helper()
	request := &proto.ReadRequest{Shard: &shard}
	for _, key := range keys {
		request.Gets = append(request.Gets, &proto.GetRequest{Key: key, ComparisonType: proto.KeyComparisonType_EQUAL})
	}
	stream, err := rpc.Read(c.ctx, request)
	if err != nil {
		c.t.Fatalf("%s: placement read failed on shard %d: %v", c.data.Pos, shard, err)
	}
	var responses []*proto.GetResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			c.t.Fatalf("%s: placement receive failed on shard %d: %v", c.data.Pos, shard, err)
		}
		responses = append(responses, response.GetGets()...)
	}
	if len(responses) != len(keys) {
		c.t.Fatalf("%s: placement shard %d returned %d responses for %d keys",
			c.data.Pos, shard, len(responses), len(keys))
	}
	return responses
}

func placementPresent(c command, response *proto.GetResponse, key string, shard int64) bool {
	c.t.Helper()
	if response == nil {
		c.t.Fatalf("%s: placement shard %d returned a nil response for %q", c.data.Pos, shard, key)
	}
	switch response.Status {
	case proto.Status_KEY_NOT_FOUND:
		return false
	case proto.Status_OK:
		if response.Version == nil || (response.Key != nil && response.GetKey() != key) {
			c.t.Fatalf("%s: placement shard %d returned a malformed record for %q", c.data.Pos, shard, key)
		}
		return true
	default:
		c.t.Fatalf("%s: placement shard %d returned unexpected status %s for %q",
			c.data.Pos, shard, response.Status, key)
		return false
	}
}

func checkPlacement(c command, assignments []*proto.ShardAssignment, keys []string,
	locations map[string][]int64) (map[int64]bool, bool) {
	c.t.Helper()
	used := make(map[int64]bool)
	overridden := false
	for _, key := range keys {
		actual := locations[key]
		if len(actual) != 1 {
			c.t.Fatalf("%s: placement key %q exists on %d shards: %v", c.data.Pos, key, len(actual), actual)
		}
		defaultShard := placementShard(c, assignments, key)
		expectedShard := defaultShard
		if c.data.HasArg("partition") {
			expectedShard = placementShard(c, assignments, c.arg("partition"))
		}
		if actual[0] != expectedShard {
			c.t.Fatalf("%s: placement key %q is on shard %d, expected %d (default=%d)",
				c.data.Pos, key, actual[0], expectedShard, defaultShard)
		}
		used[actual[0]] = true
		overridden = overridden || actual[0] != defaultShard
		c.t.Logf("placement key=%q shard=%d expected=%d default=%d", key, actual[0], expectedShard, defaultShard)
	}
	return used, overridden
}

func placementShard(c command, assignments []*proto.ShardAssignment, routingKey string) int64 {
	c.t.Helper()
	// Use XXHASH3 directly, without the SDK's hash or shard-selection helpers.
	code := uint32(xxh3.HashString(routingKey))
	for _, assignment := range assignments {
		rangeBounds := assignment.GetInt32HashRange()
		if rangeBounds.MinHashInclusive <= code && code <= rangeBounds.MaxHashInclusive {
			return assignment.Shard
		}
	}
	c.t.Fatalf("%s: no advertised placement shard for hash %d", c.data.Pos, code)
	return 0
}
