// Copyright 2017 TiKV Project Authors.
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
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tools/pd-analysis/analysis"
	"github.com/tikv/pd/tools/pd-simulator/simulator"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

var (
	pdAddr                      = flag.String("pd", "", "pd address")
	configFile                  = flag.String("config", "conf/simconfig.toml", "config file")
	caseName                    = flag.String("case", "", "case name")
	serverLogLevel              = flag.String("serverLog", "info", "pd server log level")
	simLogLevel                 = flag.String("simLog", "info", "simulator log level")
	simLogFile                  = flag.String("log-file", "", "simulator log file")
	regionNum                   = flag.Int("regionNum", 0, "regionNum of one store")
	storeNum                    = flag.Int("storeNum", 0, "storeNum")
	enableTransferRegionCounter = flag.Bool("enableTransferRegionCounter", false, "enableTransferRegionCounter")
	statusAddress               = flag.String("status-addr", "0.0.0.0:20180", "status address")
)

func main() {
	// wait PD start. Otherwise it will happen error when getting cluster ID.
	time.Sleep(3 * time.Second)
	// ignore some undefined flag
	flag.CommandLine.ParseErrorsWhitelist.UnknownFlags = true
	flag.Parse()

	simutil.InitLogger(*simLogLevel, *simLogFile)
	simutil.InitCaseConfig(*storeNum, *regionNum, *enableTransferRegionCounter)
	statistics.Denoising = false
	if simutil.CaseConfigure.EnableTransferRegionCounter {
		analysis.GetTransferCounter().Init(simutil.CaseConfigure.StoreNum, simutil.CaseConfigure.RegionNum)
	}

	schedulers.Register() // register schedulers, which is needed by simConfig.Adjust
	simConfig := simulator.NewSimConfig(*serverLogLevel)
	var meta toml.MetaData
	var err error
	if *configFile != "" {
		if meta, err = toml.DecodeFile(*configFile, simConfig); err != nil {
			simutil.Logger.Fatal("failed to decode file ", zap.Error(err))
		}
	}
	if err = simConfig.Adjust(&meta); err != nil {
		simutil.Logger.Fatal("failed to adjust simulator configuration", zap.Error(err))
	}
	if len(*caseName) == 0 {
		*caseName = simConfig.CaseName
	}

	if *caseName == "" {
		if *pdAddr != "" {
			simutil.Logger.Fatal("need to specify one config name")
		}
		for simCase := range cases.CaseMap {
			run(simCase, simConfig)
		}
	} else {
		run(*caseName, simConfig)
	}
}

func run(simCase string, simConfig *simulator.SimConfig) {
	if *pdAddr != "" {
		go runHTTPServer()
		simStart(*pdAddr, simCase, simConfig)
	} else {
		local, clean := NewSingleServer(context.Background(), simConfig)
		err := local.Run()
		if err != nil {
			simutil.Logger.Fatal("run server error", zap.Error(err))
		}
		for {
			if !local.IsClosed() && local.GetMember().IsLeader() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		simStart(local.GetAddr(), simCase, simConfig, clean)
	}
}

func runHTTPServer() {
	http.Handle("/metrics", promhttp.Handler())
	// profile API
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.Handle("/pprof/heap", pprof.Handler("heap"))
	http.Handle("/pprof/mutex", pprof.Handler("mutex"))
	http.Handle("/pprof/allocs", pprof.Handler("allocs"))
	http.Handle("/pprof/block", pprof.Handler("block"))
	http.Handle("/pprof/goroutine", pprof.Handler("goroutine"))
	// nolint
	http.ListenAndServe(*statusAddress, nil)
}

// NewSingleServer creates a pd server for simulator.
func NewSingleServer(ctx context.Context, simConfig *simulator.SimConfig) (*server.Server, testutil.CleanupFunc) {
	err := logutil.SetupLogger(simConfig.ServerConfig.Log, &simConfig.ServerConfig.Logger, &simConfig.ServerConfig.LogProps)
	if err == nil {
		log.ReplaceGlobals(simConfig.ServerConfig.Logger, simConfig.ServerConfig.LogProps)
	} else {
		log.Fatal("setup logger error", zap.Error(err))
	}

	s, err := server.CreateServer(ctx, simConfig.ServerConfig, nil, api.NewHandler)
	if err != nil {
		panic("create server failed")
	}

	cleanup := func() {
		s.Close()
		cleanServer(simConfig.ServerConfig)
	}
	return s, cleanup
}

func cleanServer(cfg *config.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}

func simStart(pdAddr string, simCase string, simConfig *simulator.SimConfig, clean ...testutil.CleanupFunc) {
	start := time.Now()
	driver, err := simulator.NewDriver(pdAddr, simCase, simConfig)
	if err != nil {
		simutil.Logger.Fatal("create driver error", zap.Error(err))
	}

	err = driver.Prepare()
	if err != nil {
		simutil.Logger.Fatal("simulator prepare error", zap.Error(err))
	}
	tickInterval := simConfig.SimTickInterval.Duration

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	simResult := "FAIL"

EXIT:
	for {
		select {
		case <-tick.C:
			driver.Tick()
			if driver.Check() {
				simResult = "OK"
				break EXIT
			}
		case <-sc:
			break EXIT
		}
	}

	driver.Stop()
	if len(clean) != 0 && clean[0] != nil {
		clean[0]()
	}

	fmt.Printf("%s [%s] total iteration: %d, time cost: %v\n", simResult, simCase, driver.TickCount(), time.Since(start))
	if analysis.GetTransferCounter().IsValid {
		analysis.GetTransferCounter().PrintResult()
	}

	if simResult != "OK" {
		os.Exit(1)
	}
}
