// Copyright 2023-2025 The Oxia Authors
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

package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/file"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/common/logging"
)

var (
	logLevelStr string
	rootCmd     = &cobra.Command{
		Use:               "oxia-maelstrom",
		Short:             "Run oxia in Maelstrom mode",
		Long:              `Run oxia in Maelstrom mode`,
		PersistentPreRunE: configureLogLevel,
	}
)

type LogLevelError string

func (l LogLevelError) Error() string {
	return fmt.Sprintf("unknown log level (%s)", string(l))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", logging.DefaultLogLevel.String(), "Set logging level [debug|info|warn|error]")
	rootCmd.PersistentFlags().BoolVarP(&logging.LogJSON, "log-json", "j", false, "Print logs in JSON format")
}

func configureLogLevel(_ *cobra.Command, _ []string) error {
	logLevel, err := logging.ParseLogLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	logging.LogLevel = logLevel
	logging.ConfigureLogger()
	return nil
}

var thisNode string
var allNodes []string

func handleInit(scanner *bufio.Scanner) {
	for {
		if err := receiveInit(scanner); err != nil {
			continue
		}

		return
	}
}

func receiveInit(scanner *bufio.Scanner) error {
	if !scanner.Scan() {
		slog.Error("no init received")
		os.Exit(1)
	}

	line := scanner.Text()
	slog.Info(
		"Got line",
		slog.Any("line", []byte(line)),
	)
	reqType, req, _ := parseRequest(line)
	if reqType != MsgTypeInit {
		slog.Error(
			"Unexpected request while waiting for init",
			slog.Any("req", req),
		)
		return errors.New("invalid message type")
	}

	init := req.(*Message[Init])

	thisNode = init.Body.NodeId
	allNodes = init.Body.NodesIDs

	slog.Info(
		"Received init request",
		slog.String("this-node", thisNode),
		slog.Any("all-nodes", allNodes),
	)

	sendResponse(Message[EmptyResponse]{
		Src:  thisNode,
		Dest: init.Src,
		Body: EmptyResponse{BaseMessageBody{
			Type:      "init_ok",
			MsgId:     msgIdGenerator.Add(1),
			InReplyTo: &init.Body.MsgId,
		}},
	})

	return nil
}

func main() {
	logging.ConfigureLogger()
	// NOTE: we must change the default logger to use Stderr for output,
	// because stdout is used as communication channel, see `sendErrorWithCode` for
	// more details.
	slog.SetDefault(slog.New(slog.NewJSONHandler(
		os.Stderr,
		&slog.HandlerOptions{},
	)))

	path, _ := os.Getwd()
	slog.Info(
		"Starting Oxia in Maelstrom mode",
		slog.String("PWD", path),
	)
	scanner := bufio.NewScanner(os.Stdin)
	handleInit(scanner)

	// Start event loop to handle requests
	grpcProvider := newMaelstromGrpcProvider()
	replicationGrpcProvider := newMaelstromReplicationRpcProvider()
	dispatcher := newDispatcher(grpcProvider, replicationGrpcProvider)

	var servers []*commonproto.DataServerIdentity
	for _, node := range allNodes {
		if node != thisNode {
			servers = append(servers, &commonproto.DataServerIdentity{
				Public:   node,
				Internal: node,
			})
		}
	}

	dataDir, err := os.MkdirTemp("", "oxia-maelstrom")
	if err != nil {
		slog.Error(
			"failed to create data dir",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	if thisNode == "n1" {
		// First node is going to be the "coordinator"
		clusterConfig := &commonproto.ClusterConfiguration{
			Namespaces: []*commonproto.Namespace{{
				Name:              constant.DefaultNamespace,
				ReplicationFactor: 3,
				InitialShardCount: 1,
			}},
			Servers: servers,
		}

		metadataProvider, err := file.NewProvider(context.Background(), filepath.Join(dataDir, "cluster-status.json"), provider.ClusterStatusCodec, provider.WatchDisabled)
		if err != nil {
			slog.Error(
				"failed to create coordinator metadata provider",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
		configProvider := memory.NewProvider(provider.ClusterConfigCodec)
		_, err = configProvider.Store(clusterConfig, provider.NotExists)
		if err != nil {
			slog.Error(
				"failed to seed coordinator config provider",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
		metadataFactory := coordmetadata.NewFactoryWithProviders(
			metadataProvider,
			configProvider,
		)
		metadata, err := metadataFactory.CreateMetadata(context.Background())
		if err != nil {
			slog.Error(
				"failed to create coordinator metadata",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
		_, err = coordruntime.New(
			metadata,
			func(instanceID string) rpc.Provider {
				return newRpcProvider(dispatcher)
			})
		if err != nil {
			slog.Error(
				"failed to create coordinator",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
	} else {
		// Any other node will be a storage node
		dataServerOption := option.NewDefaultOptions()
		dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
		dataServerOption.Storage.Database.Dir = filepath.Join(dataDir, thisNode, "db")
		dataServerOption.Storage.WAL.Dir = filepath.Join(dataDir, thisNode, "wal")
		manifest, err := manifestpkg.NewManifest(dataServerOption.Storage.Database.Dir)
		if err != nil {
			slog.Error(
				"failed to create dataserver manifest",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
		_, err = dataserver.NewWithGrpcProvider(
			context.Background(),
			commonwatch.New(dataServerOption),
			grpcProvider,
			replicationGrpcProvider,
			manifest,
			true,
		)
		if err != nil {
			return
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		rt, req, protoMsg := parseRequest(line)

		dispatcher.ReceivedMessage(rt, req, protoMsg)
	}
}
