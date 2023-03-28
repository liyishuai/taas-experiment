module github.com/tools/pd-tso-bench

go 1.16

require (
	github.com/influxdata/tdigest v0.0.1
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3
	github.com/prometheus/client_golang v1.11.1
	github.com/tikv/pd/client v0.0.0-00010101000000-000000000000
	gitlab.alibaba-inc.com/zelu.wjz/taasplugin v0.0.0-20230327173335-41480a200cb0 // indirect
	go.uber.org/zap v1.20.0
)

replace github.com/tikv/pd/client => ../../client
