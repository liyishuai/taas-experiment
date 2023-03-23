// Copyright 2021 TiKV Project Authors.
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

package pdctl

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	cmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"go.uber.org/zap"
)

func TestSendAndGetComponent(t *testing.T) {
	re := require.New(t)
	handler := func(ctx context.Context, s *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
			component := apiutil.GetComponentNameOnHTTP(r)
			for k := range r.Header {
				log.Info("header", zap.String("key", k))
			}
			log.Info("component", zap.String("component", component))
			re.Equal("pdctl", component)
			fmt.Fprint(w, component)
		})
		info := apiutil.APIServiceGroup{
			IsCore: true,
		}
		return mux, info, nil
	}
	cfg := server.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg, nil, handler)
	re.NoError(err)
	err = svr.Run()
	re.NoError(err)
	pdAddr := svr.GetAddr()
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()

	cmd := cmd.GetRootCmd()
	args := []string{"-u", pdAddr, "health"}
	output, err := ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal("pdctl\n", string(output))
}
