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

package createschema

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"

	"github.com/invopop/jsonschema"
	"github.com/spf13/cobra"

	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

var (
	outputDir string
	Cmd       = &cobra.Command{
		Use:   "create-schema",
		Short: "Create configuration schema",
		Long:  `Create configuration schema`,
		RunE:  exec,
	}
)

func init() {
	Cmd.Flags().StringVarP(&outputDir, "output-dir", "o", "", "The output file directory")
}

func exec(*cobra.Command, []string) error {
	slog.Info("Generating coordinator configuration schema")
	coordinatorOptions := jsonschema.Reflect(&coordinatoroption.Options{})
	if err := parseThenWrite(coordinatorOptions, "coordinator"); err != nil {
		return err
	}
	slog.Info("Generated coordinator configuration schema")
	slog.Info("Generating data server configuration schema")
	dataServerOptions := jsonschema.Reflect(&dataserveroption.Options{})
	if err := parseThenWrite(dataServerOptions, "dataserver"); err != nil {
		return err
	}
	slog.Info("Generated data server configuration schema")
	return nil
}

func parseThenWrite(reflect *jsonschema.Schema, name string) error {
	marshal, err := json.MarshalIndent(reflect, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path.Join(outputDir, fmt.Sprintf("%s.json", name)), marshal, 0600)
}
