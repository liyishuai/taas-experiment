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

package pd

import "github.com/prometheus/client_golang/prometheus"

var (
	cmdDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "cmd",
			Name:      "handle_cmds_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled success cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	cmdFailedDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "cmd",
			Name:      "handle_failed_cmds_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of failed handled cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "request",
			Name:      "handle_requests_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})

	tsoBestBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "request",
			Name:      "handle_tso_best_batch_size",
			Help:      "Bucketed histogram of the best batch size of handled requests.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		})

	tsoBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "request",
			Name:      "handle_tso_batch_size",
			Help:      "Bucketed histogram of the batch size of handled requests.",
			Buckets:   []float64{1, 2, 4, 8, 10, 14, 18, 22, 26, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 110, 120, 140, 160, 180, 200, 500, 1000},
		})

	tsoBatchSendLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd_client",
			Subsystem: "request",
			Name:      "tso_batch_send_latency",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "tso batch send latency",
		})

	requestForwarded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd_client",
			Subsystem: "request",
			Name:      "forwarded_status",
			Help:      "The status to indicate if the request is forwarded",
		}, []string{"host", "delegate"})
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	cmdDurationWait                     = cmdDuration.WithLabelValues("wait")
	cmdDurationTSO                      = cmdDuration.WithLabelValues("tso")
	cmdDurationTSOAsyncWait             = cmdDuration.WithLabelValues("tso_async_wait")
	cmdDurationGetRegion                = cmdDuration.WithLabelValues("get_region")
	cmdDurationGetAllMembers            = cmdDuration.WithLabelValues("get_member_info")
	cmdDurationGetPrevRegion            = cmdDuration.WithLabelValues("get_prev_region")
	cmdDurationGetRegionByID            = cmdDuration.WithLabelValues("get_region_byid")
	cmdDurationScanRegions              = cmdDuration.WithLabelValues("scan_regions")
	cmdDurationGetStore                 = cmdDuration.WithLabelValues("get_store")
	cmdDurationGetAllStores             = cmdDuration.WithLabelValues("get_all_stores")
	cmdDurationUpdateGCSafePoint        = cmdDuration.WithLabelValues("update_gc_safe_point")
	cmdDurationUpdateServiceGCSafePoint = cmdDuration.WithLabelValues("update_service_gc_safe_point")
	cmdDurationScatterRegion            = cmdDuration.WithLabelValues("scatter_region")
	cmdDurationScatterRegions           = cmdDuration.WithLabelValues("scatter_regions")
	cmdDurationGetOperator              = cmdDuration.WithLabelValues("get_operator")
	cmdDurationSplitRegions             = cmdDuration.WithLabelValues("split_regions")
	cmdDurationSplitAndScatterRegions   = cmdDuration.WithLabelValues("split_and_scatter_regions")
	cmdDurationLoadKeyspace             = cmdDuration.WithLabelValues("load_keyspace")
	cmdDurationUpdateKeyspaceState      = cmdDuration.WithLabelValues("update_keyspace_state")
	cmdDurationGet                      = cmdDuration.WithLabelValues("get")
	cmdDurationPut                      = cmdDuration.WithLabelValues("put")

	cmdFailDurationGetRegion                  = cmdFailedDuration.WithLabelValues("get_region")
	cmdFailDurationTSO                        = cmdFailedDuration.WithLabelValues("tso")
	cmdFailDurationGetAllMembers              = cmdFailedDuration.WithLabelValues("get_member_info")
	cmdFailDurationGetPrevRegion              = cmdFailedDuration.WithLabelValues("get_prev_region")
	cmdFailedDurationGetRegionByID            = cmdFailedDuration.WithLabelValues("get_region_byid")
	cmdFailedDurationScanRegions              = cmdFailedDuration.WithLabelValues("scan_regions")
	cmdFailedDurationGetStore                 = cmdFailedDuration.WithLabelValues("get_store")
	cmdFailedDurationGetAllStores             = cmdFailedDuration.WithLabelValues("get_all_stores")
	cmdFailedDurationUpdateGCSafePoint        = cmdFailedDuration.WithLabelValues("update_gc_safe_point")
	cmdFailedDurationUpdateServiceGCSafePoint = cmdFailedDuration.WithLabelValues("update_service_gc_safe_point")
	cmdFailedDurationLoadKeyspace             = cmdDuration.WithLabelValues("load_keyspace")
	cmdFailedDurationUpdateKeyspaceState      = cmdDuration.WithLabelValues("update_keyspace_state")
	requestDurationTSO                        = requestDuration.WithLabelValues("tso")
	cmdFailedDurationGet                      = cmdFailedDuration.WithLabelValues("get")
	cmdFailedDurationPut                      = cmdFailedDuration.WithLabelValues("put")
)

func init() {
	prometheus.MustRegister(cmdDuration)
	prometheus.MustRegister(cmdFailedDuration)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(tsoBestBatchSize)
	prometheus.MustRegister(tsoBatchSize)
	prometheus.MustRegister(tsoBatchSendLatency)
	prometheus.MustRegister(requestForwarded)
}
