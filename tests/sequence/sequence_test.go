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

package sequence

import (
	"fmt"
	"testing"

	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxia"
)

func TestSequence_WithOtherKeyInBatch(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)
	defer client.Close()

	for i := 1; i <= 3; i++ {
		if i == 2 {
			_ = client.Put("/abc", []byte("0"),
				oxia.PartitionKey("x"))
		}

		ch := client.Put("x", []byte("0"), oxia.PartitionKey("x"), oxia.SequenceKeysDeltas(1))
		pr := <-ch
		assert.NoError(t, pr.Err)
		assert.Equal(t, fmt.Sprintf("x-0000000000000000000%d", i), pr.Key)
	}
}
