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

package parse

import (
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/proto"
)

const AntiAffinityClearValue = "__oxia_clear_anti_affinities__"

func AntiAffinities(values []string) ([]*proto.AntiAffinity, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) == 1 && values[0] == AntiAffinityClearValue {
		return []*proto.AntiAffinity{}, nil
	}

	antiAffinities := make([]*proto.AntiAffinity, 0, len(values))
	for _, value := range values {
		if value == AntiAffinityClearValue {
			return nil, errors.New("anti-affinity without value cannot be combined with anti-affinity rules")
		}
		labelsValue, modeValue, ok := strings.Cut(value, "=")
		if !ok {
			return nil, errors.Errorf("invalid anti-affinity %q, expected labels=mode", value)
		}

		labels, err := antiAffinityLabels(labelsValue)
		if err != nil {
			return nil, err
		}

		mode := proto.ParseAntiAffinityMode(strings.TrimSpace(modeValue))
		if mode == proto.AntiAffinityModeUnknown {
			return nil, errors.Errorf(`invalid anti-affinity mode %q, must be one of "strict" or "relaxed"`, modeValue)
		}

		antiAffinities = append(antiAffinities, &proto.AntiAffinity{
			Labels: labels,
			Mode:   mode,
		})
	}
	return antiAffinities, nil
}

func antiAffinityLabels(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		label := strings.TrimSpace(part)
		if label == "" {
			return nil, errors.Errorf("invalid anti-affinity labels %q, expected comma-separated labels", value)
		}
		labels = append(labels, label)
	}
	return labels, nil
}
