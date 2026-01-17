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

// Package codec provides configuration handling utilities for various codec implementations.
package codec

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

// ConfigurableOptions defines the interface for configuration objects that can be
// loaded from files and validated. This interface ensures that all configuration
// structs provide consistent behavior for setting defaults and validation.
type ConfigurableOptions interface {
	// WithDefault sets default values for any unset configuration options.
	// This method should be called after loading configuration to ensure
	// all required fields have sensible defaults.
	WithDefault()

	// Validate checks that the configuration is valid and complete.
	// Returns an error if any required fields are missing or invalid.
	Validate() error
}

// TryReadAndInitConf reads a YAML configuration file from the given path and populates
// the provided ConfigurableOptions.
func TryReadAndInitConf(path string, configurationOptions ConfigurableOptions) error {
	if configurationOptions == nil {
		return errors.New("configuration options cannot be nil")
	}
	file, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(file, configurationOptions); err != nil {
		return err
	}
	configurationOptions.WithDefault()
	return configurationOptions.Validate()
}

func WatchFileOptions(path string) {

}
