package server

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"oxia/common"
)

var (
	conf = serverConfig{}

	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a storage node",
		Long:  `Long description`,
		Run:   main,
	}
)

func init() {
	Cmd.Flags().IntVarP(&conf.PublicServicePort, "public-port", "p", 9190, "Public service port")
	Cmd.Flags().IntVarP(&conf.InternalServicePort, "internal-port", "i", 8190, "Internal service port")
	Cmd.Flags().IntVarP(&conf.MetricsPort, "metrics-port", "m", 8080, "Metrics port")
	Cmd.Flags().StringVar(&conf.AdvertisedPublicAddress, "advertised-public-address", "", "Advertised public address")
}

func main(cmd *cobra.Command, args []string) {
	common.ConfigureLogger()

	server, err := newServer(conf)
	if err != nil {
		log.Fatal().Err(err).
			Msg("Failed to start the server")
	}

	common.WaitUntilSignal(server)
}
