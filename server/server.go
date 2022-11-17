package server

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"oxia/common"
)

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int

	AdvertisedInternalAddress string
	AdvertisedPublicAddress   string
}

type server struct {
	*internalRpcServer
	*PublicRpcServer

	shardsDirector ShardsDirector
	clientPool     common.ClientPool
}

func NewServer(config *serverConfig) (*server, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia server")

	s := &server{
		clientPool: common.NewClientPool(),
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	advertisedInternalAddress := config.AdvertisedInternalAddress
	if advertisedInternalAddress == "" {
		advertisedInternalAddress = hostname
	}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		advertisedPublicAddress = hostname
	}

	identityAddr := fmt.Sprintf("%s:%d", advertisedInternalAddress, config.InternalServicePort)
	s.shardsDirector = NewShardsDirector(identityAddr)

	s.internalRpcServer, err = newCoordinationRpcServer(config.InternalServicePort, advertisedInternalAddress, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = NewPublicRpcServer(config.PublicServicePort, advertisedPublicAddress, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Close() error {
	if err := s.PublicRpcServer.Close(); err != nil {
		return err
	}

	if err := s.internalRpcServer.Close(); err != nil {
		return err
	}

	if err := s.clientPool.Close(); err != nil {
		return err
	}

	if err := s.shardsDirector.Close(); err != nil {
		return err
	}

	return nil
}
