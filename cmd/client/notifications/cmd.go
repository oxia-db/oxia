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

package notifications

import (
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var Cmd = &cobra.Command{
	Use:   "notifications",
	Short: "Get notifications stream",
	Long:  `Follow the change notifications stream`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(_ *cobra.Command, _ []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	defer client.Close()

	notifications, err := client.GetNotifications()
	if err != nil {
		return err
	}

	defer notifications.Close()

	for notification := range notifications.Ch() {
		args := []any{
			slog.String("type", notification.Type.String()),
			slog.String("key", notification.Key),
		}
		if notification.Type == oxia.KeyCreated || notification.Type == oxia.KeyModified {
			args = append(args, slog.Int64("version-id", notification.VersionId))
		}
		if notification.Type == oxia.KeyRangeRangeDeleted {
			args = append(args, slog.String("key-range-end", notification.KeyRangeEnd))
		}
		slog.Info("", args...)
	}

	return nil
}
