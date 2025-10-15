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

//revive:disable-next-line:var-naming
package common

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWriteOutput(t *testing.T) {
	for _, test := range []struct {
		name     string
		result   any
		expected string
	}{
		{"common.OutputError", OutputError{Err: "hello"}, "{\"error\":\"hello\"}\n"},
		{"common.OutputErrorEmpty", OutputError{}, "{}\n"},
		{"common.OutputVersion", OutputVersion{
			Key:                "my-key",
			VersionId:          1,
			CreatedTimestamp:   time.UnixMilli(2),
			ModifiedTimestamp:  time.UnixMilli(3),
			ModificationsCount: 1,
			SessionId:          5,
		}, "{\"key\":\"my-key\",\"version_id\":1,\"created_timestamp\":\"" + time.UnixMilli(2).Format(time.RFC3339Nano) +
			"\",\"modified_timestamp\":\"" + time.UnixMilli(3).Format(time.RFC3339Nano) +
			"\",\"modifications_count\":1,\"ephemeral\":false,\"session_id\":5,\"client_identity\":\"\"}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := bytes.NewBufferString("")
			WriteOutput(b, test.result)
			assert.Equal(t, test.expected, b.String())
		})
	}
}
