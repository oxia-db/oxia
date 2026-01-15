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

// ReadConf reads a YAML configuration file from the given path and populates
// the provided ConfigurableOptions. After loading the configuration,
// it applies default values and validates the configuration.
func ReadConf(path string, configurationOptions ConfigurableOptions) error {
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
	if err = configurationOptions.Validate(); err != nil {
		return err
	}
	return nil
}
