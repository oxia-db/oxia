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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

// Standalone exposes only the default namespace. Namespace isolation therefore
// uses the coordinator-backed RF3 topology rather than simulating namespaces.
func TestKVNamespaceScripts(t *testing.T) {
	for _, sorting := range []string{"hierarchical", "natural"} {
		for _, shards := range []uint32{1, 4} {
			t.Run(fmt.Sprintf("%s/shards-%d", sorting, shards), func(t *testing.T) {
				keySorting, err := proto.ParseKeySortingType(sorting)
				require.NoError(t, err)
				datadriven.Walk(t, filepath.Join("testdata", "namespace"), func(t *testing.T, path string) {
					t.Helper()
					r := newRunner(t, shards, keySorting, replicatedTopology, "alpha", "beta")
					datadriven.RunTest(t, path, r.run)
				})
			})
		}
	}
}
