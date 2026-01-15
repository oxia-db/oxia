package config

import (
	"github.com/oxia-db/oxia/cmd/config/createschema"
	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use:   "config",
		Short: "Config Utils",
		Long:  `Oxia configuration utilities`,
	}
)

func init() {
	Cmd.AddCommand(createschema.Cmd)
}
