package createschema

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"

	"github.com/invopop/jsonschema"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/spf13/cobra"
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
	println(string(marshal))
	if err = os.WriteFile(path.Join(outputDir, fmt.Sprintf("%s.json", name)), marshal, os.ModePerm); err != nil {
		return err
	}
	return nil
}
