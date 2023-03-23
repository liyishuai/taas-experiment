// Copyright 2016 TiKV Project Authors.
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
	"context"
	"os"
	"os/signal"
	"syscall"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/autoscaling"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/errs"
	resource_manager "github.com/tikv/pd/pkg/mcs/resource_manager/server"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/swaggerserver"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/apiv2"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/join"
	"go.uber.org/zap"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "pd-server",
		Short: "Placement Driver server",
		Run:   createServerWrapper,
	}

	addFlags(rootCmd)
	rootCmd.AddCommand(NewServiceCommand())

	rootCmd.SetOutput(os.Stdout)
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
		os.Exit(1)
	}
}

// NewServiceCommand returns the service command.
func NewServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "services <mode>",
		Short: "Run services, for example, tso, resource_manager",
	}
	cmd.AddCommand(NewTSOServiceCommand())
	cmd.AddCommand(NewResourceManagerServiceCommand())
	cmd.AddCommand(NewAPIServiceCommand())
	return cmd
}

// NewTSOServiceCommand returns the tso service command.
func NewTSOServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tso",
		Short: "Run the TSO service",
		Run:   tso.CreateServerWrapper,
	}
	cmd.Flags().BoolP("version", "V", false, "print version information and exit")
	cmd.Flags().StringP("config", "", "", "config file")
	cmd.Flags().StringP("backend-endpoints", "", "", "url for etcd client")
	cmd.Flags().StringP("listen-addr", "", "", "listen address for tso service")
	cmd.Flags().StringP("advertise-listen-addr", "", "", "advertise urls for listen address (default '${listen-addr}')")
	cmd.Flags().StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	cmd.Flags().StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	cmd.Flags().StringP("key", "", "", "path of file that contains X509 key in PEM format")
	return cmd
}

// NewResourceManagerServiceCommand returns the resource manager service command.
func NewResourceManagerServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource-manager",
		Short: "Run the resource manager service",
		Run:   resource_manager.CreateServerWrapper,
	}
	cmd.Flags().BoolP("version", "V", false, "print version information and exit")
	cmd.Flags().StringP("config", "", "", "config file")
	cmd.Flags().StringP("backend-endpoints", "", "", "url for etcd client")
	cmd.Flags().StringP("listen-addr", "", "", "listen address for resource management service")
	cmd.Flags().StringP("advertise-listen-addr", "", "", "advertise urls for listen address (default '${listen-addr}')")
	cmd.Flags().StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	cmd.Flags().StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	cmd.Flags().StringP("key", "", "", "path of file that contains X509 key in PEM format")
	return cmd
}

// NewAPIServiceCommand returns the API service command.
func NewAPIServiceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Run the API service",
		Run:   createAPIServerWrapper,
	}
	addFlags(cmd)
	return cmd
}

func addFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("version", "V", false, "print version information and exit")
	cmd.Flags().StringP("config", "", "", "config file")
	cmd.Flags().BoolP("config-check", "", false, "check config file validity and exit")
	cmd.Flags().StringP("name", "", "", "human-readable name for this pd member")
	cmd.Flags().StringP("data-dir", "", "", "path to the data directory (default 'default.${name}')")
	cmd.Flags().StringP("client-urls", "", "", "urls for client traffic")
	cmd.Flags().StringP("advertise-client-urls", "", "", "advertise urls for client traffic (default '${client-urls}')")
	cmd.Flags().StringP("peer-urls", "", "", "urls for peer traffic")
	cmd.Flags().StringP("advertise-peer-urls", "", "", "advertise urls for peer traffic (default '${peer-urls}')")
	cmd.Flags().StringP("initial-cluster", "", "", "initial cluster configuration for bootstrapping, e,g. pd=http://127.0.0.1:2380")
	cmd.Flags().StringP("join", "", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")
	cmd.Flags().StringP("metrics-addr", "", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	cmd.Flags().StringP("log-level", "L", "info", "log level: debug, info, warn, error, fatal (default 'info')")
	cmd.Flags().StringP("log-file", "", "", "log file path")
	cmd.Flags().StringP("cacert", "", "", "path of file that contains list of trusted TLS CAs")
	cmd.Flags().StringP("cert", "", "", "path of file that contains X509 certificate in PEM format")
	cmd.Flags().StringP("key", "", "", "path of file that contains X509 key in PEM format")
	cmd.Flags().BoolP("force-new-cluster", "", false, "force to create a new one-member cluster")
}

func createAPIServerWrapper(cmd *cobra.Command, args []string) {
	start(cmd, args, cmd.CalledAs())
}

func createServerWrapper(cmd *cobra.Command, args []string) {
	start(cmd, args)
}

func start(cmd *cobra.Command, args []string, services ...string) {
	schedulers.Register()
	cfg := config.NewConfig()
	flagSet := cmd.Flags()
	flagSet.Parse(args)
	err := cfg.Parse(flagSet)
	defer logutil.LogPanic()

	if err != nil {
		cmd.Println(err)
		return
	}

	if printVersion, err := flagSet.GetBool("version"); err != nil {
		cmd.Println(err)
		return
	} else if printVersion {
		versioninfo.Print()
		exit(0)
	}

	if configCheck, err := flagSet.GetBool("config-check"); err != nil {
		cmd.Println(err)
		return
	} else if configCheck {
		configutil.PrintConfigCheckMsg(os.Stdout, cfg.WarningMsgs)
		exit(0)
	}

	// New zap logger
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	if len(services) != 0 {
		versioninfo.Log(server.APIServiceMode)
	} else {
		versioninfo.Log(server.PDMode)
	}

	for _, msg := range cfg.WarningMsgs {
		log.Warn(msg)
	}

	// TODO: Make it configurable if it has big impact on performance.
	grpcprometheus.EnableHandlingTimeHistogram()

	metricutil.Push(&cfg.Metric)

	err = join.PrepareJoinCluster(cfg)
	if err != nil {
		log.Fatal("join meet error", errs.ZapError(err))
	}

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	serviceBuilders := []server.HandlerBuilder{api.NewHandler, apiv2.NewV2Handler, swaggerserver.NewHandler, autoscaling.NewHandler}
	serviceBuilders = append(serviceBuilders, dashboard.GetServiceBuilders()...)
	svr, err := server.CreateServer(ctx, cfg, services, serviceBuilders...)
	if err != nil {
		log.Fatal("create server failed", errs.ZapError(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", errs.ZapError(err))
	}

	<-ctx.Done()
	log.Info("got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
