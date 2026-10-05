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

package file

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/fsnotify/fsnotify"

	"github.com/oxia-db/oxia/common/channel"
)

// WatchFile signals each time the file at path is created, written, renamed
// or removed. The channel is closed when the watch fails or ctx is done.
func WatchFile(ctx context.Context, path string) (<-chan struct{}, error) {
	logger := slog.With(slog.String("component", "file-watch"), slog.String("path", path))
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := watcher.Add(filepath.Dir(path)); err != nil {
		_ = watcher.Close()
		return nil, err
	}

	changes := make(chan struct{}, 1)
	go func() {
		defer close(changes)
		defer func() {
			if err := watcher.Close(); err != nil {
				logger.Warn("Failed to close file watcher", slog.Any("error", err))
			}
		}()
		watchedPath := filepath.Clean(path)
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-watcher.Errors:
				if ok {
					logger.Warn("File watch failed", slog.Any("error", err))
				}
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if filepath.Clean(event.Name) == watchedPath &&
					event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename|fsnotify.Remove) != 0 {
					channel.PushNoBlock(changes, struct{}{})
				}
			}
		}
	}()
	return changes, nil
}
