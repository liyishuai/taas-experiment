// Copyright 2022 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/ratelimit"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type auditMiddlewareTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestAuditMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(auditMiddlewareTestSuite))
}

func (suite *auditMiddlewareTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = false
	})
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (suite *auditMiddlewareTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *auditMiddlewareTestSuite) TestConfigAuditSwitch() {
	addr := fmt.Sprintf("%s/service-middleware/config", suite.urlPrefix)
	sc := &config.ServiceMiddlewareConfig{}
	re := suite.Require()
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.True(sc.EnableAudit)

	ms := map[string]interface{}{
		"enable-audit":      "true",
		"enable-rate-limit": "true",
	}
	postData, err := json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.True(sc.EnableAudit)
	suite.True(sc.EnableRateLimit)
	ms = map[string]interface{}{
		"audit.enable-audit": "false",
		"enable-rate-limit":  "false",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.False(sc.EnableAudit)
	suite.False(sc.EnableRateLimit)

	// test empty
	ms = map[string]interface{}{}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re), tu.StringContain(re, "The input is empty.")))
	ms = map[string]interface{}{
		"audit": "false",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item audit not found")))
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail", "return(true)"))
	ms = map[string]interface{}{
		"audit.enable-audit": "true",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest)))
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail"))
	ms = map[string]interface{}{
		"audit.audit": "false",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item audit not found")))
}

type rateLimitConfigTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestRateLimitConfigTestSuite(t *testing.T) {
	suite.Run(t, new(rateLimitConfigTestSuite))
}

func (suite *rateLimitConfigTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})
	mustBootstrapCluster(re, suite.svr)
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", suite.svr.GetAddr(), apiPrefix)
}

func (suite *rateLimitConfigTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *rateLimitConfigTestSuite) TestUpdateRateLimitConfig() {
	urlPrefix := fmt.Sprintf("%s%s/api/v1/service-middleware/config/rate-limit", suite.svr.GetAddr(), apiPrefix)

	// test empty type
	input := make(map[string]interface{})
	input["type"] = 123
	jsonBody, err := json.Marshal(input)
	suite.NoError(err)
	re := suite.Require()
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "\"The type is empty.\"\n"))
	suite.NoError(err)
	// test invalid type
	input = make(map[string]interface{})
	input["type"] = "url"
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "\"The type is invalid.\"\n"))
	suite.NoError(err)

	// test empty label
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = ""
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "\"The label is empty.\"\n"))
	suite.NoError(err)
	// test no label matched
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "TestLabel"
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "\"There is no label matched.\"\n"))
	suite.NoError(err)

	// test empty path
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = ""
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "\"The path is empty.\"\n"))
	suite.NoError(err)

	// test path but no label matched
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/test"
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "\"There is no label matched.\"\n"))
	suite.NoError(err)

	// no change
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "GetHealthStatus"
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringEqual(re, "\"No changed.\"\n"))
	suite.NoError(err)

	// change concurrency
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = http.MethodGet
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Concurrency limiter is changed."))
	suite.NoError(err)
	input["concurrency"] = 0
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Concurrency limiter is deleted."))
	suite.NoError(err)

	// change qps
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = http.MethodGet
	input["qps"] = 100
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "QPS rate limiter is changed."))
	suite.NoError(err)

	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/health"
	input["method"] = http.MethodGet
	input["qps"] = 0.3
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "QPS rate limiter is changed."))
	suite.NoError(err)
	suite.Equal(1, suite.svr.GetRateLimitConfig().LimiterConfig["GetHealthStatus"].QPSBurst)

	input["qps"] = -1
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "QPS rate limiter is deleted."))
	suite.NoError(err)

	// change both
	input = make(map[string]interface{})
	input["type"] = "path"
	input["path"] = "/pd/api/v1/debug/pprof/profile"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	result := rateLimitResult{}
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusOK(re), tu.StringContain(re, "Concurrency limiter is changed."),
		tu.StringContain(re, "QPS rate limiter is changed."),
		tu.ExtractJSON(re, &result),
	)
	suite.Equal(100., result.LimiterConfig["Profile"].QPS)
	suite.Equal(100, result.LimiterConfig["Profile"].QPSBurst)
	suite.Equal(uint64(100), result.LimiterConfig["Profile"].ConcurrencyLimit)
	suite.NoError(err)

	limiter := suite.svr.GetServiceRateLimiter()
	limiter.Update("SetRatelimitConfig", ratelimit.AddLabelAllowList())

	// Allow list
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "SetRatelimitConfig"
	input["qps"] = 100
	input["concurrency"] = 100
	jsonBody, err = json.Marshal(input)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, urlPrefix, jsonBody,
		tu.StatusNotOK(re), tu.StringEqual(re, "\"This service is in allow list whose config can not be changed.\"\n"))
	suite.NoError(err)
}

func (suite *rateLimitConfigTestSuite) TestConfigRateLimitSwitch() {
	addr := fmt.Sprintf("%s/service-middleware/config", suite.urlPrefix)
	sc := &config.ServiceMiddlewareConfig{}
	re := suite.Require()
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.False(sc.EnableRateLimit)

	ms := map[string]interface{}{
		"enable-rate-limit": "true",
	}
	postData, err := json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.True(sc.EnableRateLimit)
	ms = map[string]interface{}{
		"enable-rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc = &config.ServiceMiddlewareConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.False(sc.EnableRateLimit)

	// test empty
	ms = map[string]interface{}{}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re), tu.StringContain(re, "The input is empty.")))
	ms = map[string]interface{}{
		"rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item rate-limit not found")))
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail", "return(true)"))
	ms = map[string]interface{}{
		"rate-limit.enable-rate-limit": "true",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest)))
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/config/persistServiceMiddlewareFail"))
	ms = map[string]interface{}{
		"rate-limit.rate-limit": "false",
	}
	postData, err = json.Marshal(ms)
	suite.NoError(err)
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.Status(re, http.StatusBadRequest), tu.StringEqual(re, "config item rate-limit not found")))
}

func (suite *rateLimitConfigTestSuite) TestConfigLimiterConifgByOriginAPI() {
	// this test case is used to test updating `limiter-config` by origin API simply
	addr := fmt.Sprintf("%s/service-middleware/config", suite.urlPrefix)
	dimensionConfig := ratelimit.DimensionConfig{QPS: 1}
	limiterConfig := map[string]interface{}{
		"CreateOperator": dimensionConfig,
	}
	ms := map[string]interface{}{
		"limiter-config": limiterConfig,
	}
	postData, err := json.Marshal(ms)
	suite.NoError(err)
	re := suite.Require()
	suite.NoError(tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(re)))
	sc := &config.ServiceMiddlewareConfig{}
	suite.NoError(tu.ReadGetJSON(re, testDialClient, addr, sc))
	suite.Equal(1., sc.RateLimitConfig.LimiterConfig["CreateOperator"].QPS)
}
