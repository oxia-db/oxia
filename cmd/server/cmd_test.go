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

package server

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/node"
)

func TestServerCmd(t *testing.T) {
	for _, test := range []struct {
		args         []string
		expectedConf node.Config
		isErr        bool
	}{
		{[]string{}, node.Config{
			PublicServiceAddr:          "0.0.0.0:6648",
			InternalServiceAddr:        "0.0.0.0:6649",
			MetricsServiceAddr:         "0.0.0.0:8080",
			DataDir:                    "./data/db",
			WalDir:                     "./data/wal",
			WalRetentionTime:           1 * time.Hour,
			WalSyncData:                true,
			NotificationsRetentionTime: 1 * time.Hour,
			DbBlockCacheMB:             100,
		}, false},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			Cmd.SetArgs(test.args)
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				return nil
			}
			err := Cmd.Execute()
			assert.Equal(t, test.isErr, err != nil)
			assert.Equal(t, test.expectedConf, conf)
		})
	}
}
