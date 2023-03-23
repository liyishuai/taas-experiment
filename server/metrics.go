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

package server

import "github.com/prometheus/client_golang/prometheus"

var (
	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})
	bucketReportCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "bucket_report",
			Help:      "Counter of bucket report.",
		}, []string{"address", "store", "type", "status"})
	regionHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat",
			Help:      "Counter of region heartbeat.",
		}, []string{"address", "store", "type", "status"})

	regionHeartbeatLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "region_heartbeat_latency_seconds",
			Help:      "Bucketed histogram of latency (s) of receiving heartbeat.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{"address", "store"})

	metadataGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "cluster",
			Name:      "metadata",
			Help:      "Record critical metadata.",
		}, []string{"type"})

	etcdStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "etcd_state",
			Help:      "Etcd raft states.",
		}, []string{"type"})

	tsoProxyHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_proxy_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso proxy requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	tsoProxyBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_proxy_batch_size",
			Help:      "Bucketed histogram of the batch size of handled tso proxy requests.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		})

	tsoHandleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_tso_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled tso requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	bucketReportLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "handle_bucket_report_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled bucket report requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	bucketReportInterval = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "bucket_report_interval_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled bucket report requests.",
			Buckets:   prometheus.LinearBuckets(0, 30, 20), // 1s ~ 17m
		}, []string{"address", "store"})

	regionHeartbeatHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "handle_region_heartbeat_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled region heartbeat requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	// TODO: pre-allocate gauge metrics
	storeHeartbeatHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "handle_store_heartbeat_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled store heartbeat requests.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 29), // 0.1ms ~ 7hours
		}, []string{"address", "store"})

	serverInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "server",
			Name:      "info",
			Help:      "Indicate the pd server info, and the value is the start timestamp (s).",
		}, []string{"version", "hash"})

	serviceAuditHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "audit_handling_seconds",
			Help:      "PD server service handling audit",
			Buckets:   prometheus.DefBuckets,
		}, []string{"service", "method", "component", "ip"})
	serverMaxProcs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "service",
			Name:      "maxprocs",
			Help:      "The value of GOMAXPROCS.",
		})
)

func init() {
	prometheus.MustRegister(timeJumpBackCounter)
	prometheus.MustRegister(regionHeartbeatCounter)
	prometheus.MustRegister(regionHeartbeatLatency)
	prometheus.MustRegister(metadataGauge)
	prometheus.MustRegister(etcdStateGauge)
	prometheus.MustRegister(tsoProxyHandleDuration)
	prometheus.MustRegister(tsoProxyBatchSize)
	prometheus.MustRegister(tsoHandleDuration)
	prometheus.MustRegister(regionHeartbeatHandleDuration)
	prometheus.MustRegister(storeHeartbeatHandleDuration)
	prometheus.MustRegister(serverInfo)
	prometheus.MustRegister(bucketReportCounter)
	prometheus.MustRegister(bucketReportLatency)
	prometheus.MustRegister(serviceAuditHistogram)
	prometheus.MustRegister(bucketReportInterval)
	prometheus.MustRegister(serverMaxProcs)
}
