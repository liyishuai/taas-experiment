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

package buckets

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bucketsHotDegreeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "buckets_hot_degree_hist",
			Help:      "Bucketed histogram of bucket hot degree",
			Buckets:   prometheus.LinearBuckets(-20, 2, 20), // [-20 20]
		})

	bucketsTaskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "bucket_task_duration",
			Help:      "Bucketed histogram of processing time (s) of bucket task.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 23), // 0.1ms ~ 14m
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(bucketsHotDegreeHist)
	prometheus.MustRegister(bucketsTaskDuration)
}
