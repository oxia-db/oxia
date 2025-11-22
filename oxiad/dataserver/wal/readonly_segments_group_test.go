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

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestReadOnlySegmentsGroupTrimSegments(t *testing.T) {
	t.Run("when txnFile is not exists", func(t *testing.T) {
		walFactory := NewWalFactory(&FactoryOptions{
			BaseWalDir:  t.TempDir(),
			Retention:   1 * time.Hour,
			SegmentSize: 128,
			SyncData:    true,
		})
		w, err := walFactory.NewWal("test", 1, nil)
		assert.NoError(t, err)
		for i := int64(0); i < 1000; i++ {
			assert.NoError(t, w.Append(&proto.LogEntry{
				Term:   1,
				Offset: i,
				Value:  fmt.Appendf(nil, "test-%d", i),
			}))
		}
		walBasePath := w.(*wal).walPath
		readOnlySegments, err := newReadOnlySegmentsGroup(walBasePath)
		assert.NoError(t, err)

		// Ensure newReadOnlySegment will return an NotExists error
		err = os.Remove(filepath.Join(walBasePath, "0.txnx"))
		assert.NoError(t, err)

		err = readOnlySegments.TrimSegments(6)
		assert.NoError(t, err)
	})
}
