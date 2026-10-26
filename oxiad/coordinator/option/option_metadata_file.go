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

package option

import (
	"cmp"
	"encoding/json"
	"errors"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const (
	DefaultFileStatusName = "cluster-status.json"
	DefaultFileConfigName = "cluster.yaml"
)

// FileMetadata configures the file provider, which keeps the cluster
// configuration and status in two local files. The names of the files are
// relative to Dir, or are paths, relative to the working directory, when Dir
// is empty.
//
// The deprecated path is still accepted as the path of the status file, but
// not together with a different dir or statusName.
type FileMetadata struct {
	// Dir is the directory of the files. When it is empty, StatusName and
	// ConfigName are paths.
	Dir string `yaml:"dir,omitempty" json:"dir,omitempty" jsonschema:"description=Directory of the files. When it is empty\\, the names are paths,example=./metadata"`
	// StatusName is the name of the file of the cluster status, which the
	// coordinator writes. It defaults to cluster-status.json.
	StatusName string `yaml:"statusName,omitempty" json:"statusName,omitempty" jsonschema:"description=Name of the file of the cluster status\\, or its path when dir is empty,default=cluster-status.json,example=cluster-status.json"`
	// ConfigName is the name of the file of the cluster configuration, which
	// the coordinator reads, and watches for changes. It defaults to
	// cluster.yaml.
	ConfigName string `yaml:"configName,omitempty" json:"configName,omitempty" jsonschema:"description=Name of the file of the cluster configuration\\, or its path when dir is empty,default=cluster.yaml,example=cluster.yaml"`
}

type fileMetadataCompat struct {
	Dir        string `yaml:"dir" json:"dir"`
	StatusName string `yaml:"statusName" json:"statusName"`
	ConfigName string `yaml:"configName" json:"configName"`
	Path       string `yaml:"path" json:"path"`
}

func (fm *FileMetadata) UnmarshalJSON(data []byte) error {
	decoded := fileMetadataCompat{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	return fm.applyCompat(decoded)
}

func (fm *FileMetadata) UnmarshalYAML(value *yaml.Node) error {
	decoded := fileMetadataCompat{}
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	return fm.applyCompat(decoded)
}

func (fm *FileMetadata) applyCompat(decoded fileMetadataCompat) error {
	if decoded.Path != "" {
		if decoded.Dir != "" {
			statusDir := filepath.Dir(decoded.Path)
			statusName := filepath.Base(decoded.Path)
			if filepath.Clean(decoded.Dir) != filepath.Clean(statusDir) {
				return errors.New("file.dir and deprecated file.path are both set with different directories")
			}
			if decoded.StatusName != "" && decoded.StatusName != statusName {
				return errors.New("file.statusName and deprecated file.path are both set with different file names")
			}
			if decoded.StatusName == "" {
				decoded.StatusName = statusName
			}
		} else {
			if decoded.StatusName != "" && filepath.Clean(decoded.StatusName) != filepath.Clean(decoded.Path) {
				return errors.New("file.statusName and deprecated file.path are both set with different paths")
			}
			if decoded.StatusName == "" {
				decoded.StatusName = decoded.Path
			}
		}
	}

	fm.Dir = decoded.Dir
	fm.StatusName = decoded.StatusName
	fm.ConfigName = decoded.ConfigName
	return nil
}

func (fm *FileMetadata) applyLegacyConfigPath(configPath string) error {
	if fm.Dir != "" {
		configDir := filepath.Dir(configPath)
		configName := filepath.Base(configPath)
		if filepath.Clean(fm.Dir) != filepath.Clean(configDir) {
			return errors.New("metadata.file.dir and deprecated cluster.configPath are both set with different directories")
		}
		if fm.ConfigName != "" && fm.ConfigName != configName {
			return errors.New("metadata.file.configName and deprecated cluster.configPath are both set with different file names")
		}
		if fm.ConfigName == "" {
			fm.ConfigName = configName
		}
		return nil
	}

	if fm.ConfigName != "" && filepath.Clean(fm.ConfigName) != filepath.Clean(configPath) {
		return errors.New("metadata.file.configName and deprecated cluster.configPath are both set with different paths")
	}
	if fm.ConfigName == "" {
		fm.ConfigName = configPath
	}
	return nil
}

func (fm FileMetadata) StatusPath() string {
	return filePath(fm.Dir, cmp.Or(fm.StatusName, DefaultFileStatusName))
}

func (fm FileMetadata) ConfigPath() string {
	return filePath(fm.Dir, cmp.Or(fm.ConfigName, DefaultFileConfigName))
}

func filePath(dir, name string) string {
	if dir == "" {
		return name
	}
	return filepath.Join(dir, name)
}
