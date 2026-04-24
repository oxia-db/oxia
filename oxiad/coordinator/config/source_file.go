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

package config

import (
	"context"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type fileSource struct {
	v *viper.Viper
}

func newFileSource(configPath string) *fileSource {
	v := viper.New()
	v.SetConfigType("yaml")
	if configPath == "" {
		v.AddConfigPath("/oxia/conf")
		v.AddConfigPath(".")
	} else {
		v.SetConfigFile(configPath)
	}
	return &fileSource{v: v}
}

func (s *fileSource) Load(context.Context) ([]byte, error) {
	if err := s.v.ReadInConfig(); err != nil {
		return nil, err
	}

	configFile := s.v.ConfigFileUsed()
	if configFile == "" {
		return nil, errors.New("cluster configuration: no config file was loaded")
	}
	return os.ReadFile(configFile)
}

func (s *fileSource) Watch(ctx context.Context, events chan<- struct{}) error {
	s.v.OnConfigChange(func(_ fsnotify.Event) {
		select {
		case events <- struct{}{}:
		case <-ctx.Done():
		}
	})
	s.v.WatchConfig()
	return nil
}
