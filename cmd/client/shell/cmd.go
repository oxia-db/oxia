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

package shell

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/kballard/go-shellquote"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/cmd/client/shell/admin"
	"github.com/oxia-db/oxia/cmd/client/shell/client"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxia"
)

const (
	defaultPrompt = "oxia> "
	commandAdmin  = "admin"
)

var (
	defaultAdminAddress = fmt.Sprintf("localhost:%d", constant.DefaultAdminPort)
	Config              = flags{
		prompt:       defaultPrompt,
		adminAddress: defaultAdminAddress,
	}
	errExit = errors.New("exit")
)

type flags struct {
	prompt       string
	adminAddress string
	adminAuth    clientauth.Config
	outputFormat string
}

func (f *flags) Reset() {
	f.prompt = defaultPrompt
	f.adminAddress = defaultAdminAddress
	f.adminAuth = clientauth.Config{}
	f.outputFormat = ""
}

var Cmd = newCommand()

func newCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "shell",
		Aliases:      []string{"interactive", "terminal"},
		Short:        "Start an interactive Oxia terminal",
		Long:         `Start an interactive terminal for running Oxia client and admin commands.`,
		Args:         cobra.NoArgs,
		RunE:         exec,
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&Config.prompt, "prompt", defaultPrompt, "Prompt to show for interactive input")
	cmd.Flags().StringVar(&Config.adminAddress, "admin-address", defaultAdminAddress, "Admin client address")
	cmd.Flags().StringVar(&Config.adminAuth.Token, "admin-auth-token", "", "Bearer token for authenticated admin requests")
	cmd.Flags().StringVar(&Config.adminAuth.TokenFile, "admin-auth-token-file", "", "Path to bearer token file for authenticated admin requests")
	cmd.Flags().StringVarP(&Config.outputFormat, "output", "o", "", "Admin output format. One of: json|yaml|table")

	defaultServiceAddress := fmt.Sprintf("localhost:%d", constant.DefaultPublicPort)
	cmd.Flags().StringVarP(&common.Config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	cmd.Flags().StringVarP(&common.Config.Namespace, "namespace", "n", oxia.DefaultNamespace, "The Oxia namespace to use")
	cmd.Flags().DurationVar(&common.Config.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Requests timeout")
	cmd.Flags().StringVar(&common.Config.Auth.Token, "auth-token", "", "Bearer token for authenticated client requests")
	cmd.Flags().StringVar(&common.Config.Auth.TokenFile, "auth-token-file", "", "Path to bearer token file for authenticated client requests")

	return cmd
}

func exec(cmd *cobra.Command, _ []string) error {
	if err := admincommons.ValidateOutputFormat(Config.outputFormat); err != nil {
		return err
	}

	in := cmd.InOrStdin()
	out := cmd.OutOrStdout()
	interactive := false
	if inFile, ok := in.(*os.File); ok {
		if outFile, ok := out.(*os.File); ok {
			inStat, inErr := inFile.Stat()
			outStat, outErr := outFile.Stat()
			interactive = inErr == nil &&
				outErr == nil &&
				inStat.Mode()&os.ModeCharDevice != 0 &&
				outStat.Mode()&os.ModeCharDevice != 0
		}
	}

	session := &repl{
		ctx:          cmd.Context(),
		in:           in,
		out:          out,
		errOut:       cmd.ErrOrStderr(),
		prompt:       Config.prompt,
		interactive:  interactive,
		adminConfig:  adminClientConfig(),
		outputFormat: Config.outputFormat,
	}
	defer func() {
		if session.client != nil {
			_ = session.client.Close()
		}
		if session.admin != nil {
			_ = session.admin.Close()
		}
	}()

	if session.ctx == nil {
		session.ctx = context.Background()
	}
	if session.interactive {
		return session.runPrompt()
	}
	return session.runBuffered()
}

type repl struct {
	ctx         context.Context
	client      oxia.SyncClient
	admin       oxia.AdminClient
	in          io.Reader
	out         io.Writer
	errOut      io.Writer
	prompt      string
	interactive bool

	adminConfig  admincommons.AdminClientConfig
	outputFormat string
}

func adminClientConfig() admincommons.AdminClientConfig {
	authConfig := Config.adminAuth
	if !authConfig.Enabled() {
		authConfig = common.Config.Auth
	}
	return admincommons.AdminClientConfig{
		AdminAddress: Config.adminAddress,
		Auth:         authConfig,
	}
}

func (r *repl) clientForCommand() (oxia.SyncClient, error) {
	if r.client != nil {
		return r.client, nil
	}
	syncClient, err := common.Config.NewClient()
	if err != nil {
		return nil, err
	}
	r.client = syncClient
	return syncClient, nil
}

func isConnectionFailure(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF)
	}
}

func (r *repl) runBuffered() error {
	reader := bufio.NewReader(r.in)
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		if line == "" && errors.Is(readErr, io.EOF) {
			return nil
		}

		if err := r.executeLine(line); errors.Is(err, errExit) {
			return nil
		} else if err != nil {
			return err
		}

		if errors.Is(readErr, io.EOF) {
			return nil
		}
	}
}

func (r *repl) executeLine(line string) error {
	args, err := shellquote.Split(line)
	if err != nil {
		if errors.Is(err, shellquote.UnterminatedSingleQuoteError) ||
			errors.Is(err, shellquote.UnterminatedDoubleQuoteError) {
			err = errors.New("unterminated quoted string")
		}
		if r.interactive {
			_, writeErr := fmt.Fprintf(r.errOut, "Error: %v\n", err)
			return writeErr
		}
		return err
	}
	if len(args) == 0 {
		return nil
	}

	clientExecutor := client.Executor{
		Ctx:    r.ctx,
		Client: r.clientForCommand,
		Out:    r.out,
	}
	switch args[0] {
	case "exit", "quit":
		return errExit
	case "help", "?":
		_, err = fmt.Fprint(r.out, helpText)
	case commandAdmin:
		err = admin.Executor{
			Ctx: r.ctx,
			Client: func() (oxia.AdminClient, error) {
				if r.admin != nil {
					return r.admin, nil
				}
				adminClient, err := r.adminConfig.NewAdminClient()
				if err != nil {
					return nil, err
				}
				r.admin = adminClient
				return adminClient, nil
			},
			Out:          r.out,
			OutputFormat: r.outputFormat,
		}.Execute(args[1:])
		if isConnectionFailure(err) && r.admin != nil {
			_ = r.admin.Close()
			r.admin = nil
		}
	default:
		err = clientExecutor.Execute(args)
		if isConnectionFailure(err) && r.client != nil {
			_ = r.client.Close()
			r.client = nil
		}
	}

	if errors.Is(err, errExit) {
		return errExit
	}
	if err != nil {
		if r.interactive {
			_, writeErr := fmt.Fprintf(r.errOut, "Error: %v\n", err)
			return writeErr
		}
		return err
	}
	return nil
}

const helpText = `Available commands:
  get [--hex] [--include-version] [--partition-key KEY] [--index NAME] [--comparison-type TYPE] KEY
  put [--expected-version VERSION] [--create-only] [--ephemeral] [--partition-key KEY] [--sequence-keys-deltas CSV] [--index NAME=KEY] KEY VALUE
  delete [--expected-version VERSION] [--partition-key KEY] KEY
  delete-range [--partition-key KEY] (--key-min MIN --key-max MAX | MIN_KEY MAX_KEY)
  list [--partition-key KEY] [--index NAME] [--internal-keys] ([--key-min MIN] [--key-max MAX] | [MIN_KEY [MAX_KEY]])
  range-scan [--hex] [--include-version] [--partition-key KEY] [--index NAME] [--internal-keys] ([--key-min MIN] [--key-max MAX] | [MIN_KEY [MAX_KEY]])
  admin namespace list|get|create|patch|delete ...
  admin dataserver list|get|create|patch|delete ...
  admin shard split --shard ID [--namespace NAME] [--split-point HASH]
  help
  exit
`
