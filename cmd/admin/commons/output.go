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

package commons

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

const (
	OutputJSON  = "json"
	OutputYAML  = "yaml"
	OutputName  = "name"
	OutputTable = "table"
)

func ValidateOutputFormat(format string) error {
	switch format {
	case "", OutputJSON, OutputYAML, OutputName, OutputTable:
		return nil
	default:
		return errors.Errorf("unsupported output format %q, expected one of: json, yaml, name, table", format)
	}
}

func NormalizeOutputFormat(format string) string {
	if format == "" {
		return OutputTable
	}
	return format
}

func WriteStructuredOutput(out io.Writer, format string, value any) error {
	switch format {
	case OutputJSON:
		return writeJSON(out, value)
	case OutputYAML:
		return writeYAML(out, value)
	default:
		return errors.Errorf("unsupported structured output format %q, expected one of: json, yaml", format)
	}
}

func WriteResourceNames(out io.Writer, resource string, names []string) error {
	for _, name := range names {
		if _, err := fmt.Fprintf(out, "%s/%s\n", resource, name); err != nil {
			return err
		}
	}
	return nil
}

func NewTableWriter(out io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
}

func FormatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, labels[key]))
	}
	return strings.Join(parts, ",")
}

func ParseLabels(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return map[string]string{}, nil
	}

	labels := make(map[string]string, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		key, labelValue, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return nil, errors.Errorf("invalid label %q, expected key=value", value)
		}
		labels[key] = labelValue
	}
	return labels, nil
}

func writeJSON(out io.Writer, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s\n", data)
	return err
}

func writeYAML(out io.Writer, value any) error {
	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	_, err = out.Write(data)
	return err
}
