// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
)

const (
	// maxRetryTimes is the max retry times for initializing the cluster ID.
	maxRetryTimes = 5
	// clusterIDPath is the path to store cluster id
	clusterIDPath = "/pd/cluster_id"
	// retryInterval is the interval to retry.
	retryInterval = time.Second
)

// InitClusterID initializes the cluster ID.
func InitClusterID(ctx context.Context, client *clientv3.Client) (id uint64, err error) {
	for i := 0; i < maxRetryTimes; i++ {
		if clusterID, err := etcdutil.GetClusterID(client, clusterIDPath); err == nil && clusterID != 0 {
			return clusterID, nil
		}
		select {
		case <-ctx.Done():
			return 0, err
		case <-time.After(retryInterval):
		}
	}
	return 0, errors.Errorf("failed to init cluster ID after retrying %d times", maxRetryTimes)
}
