// Copyright 2019 TiKV Project Authors.
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

package election

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	revokeLeaseTimeout = time.Second
	requestTimeout     = etcdutil.DefaultRequestTimeout
	slowRequestTime    = etcdutil.DefaultSlowRequestTime
)

// lease is used as the low-level mechanism for campaigning and renewing elected leadership.
// The way to gain and maintain leadership is to update and keep the lease alive continuously.
type lease struct {
	// purpose is used to show what this election for
	Purpose string
	// etcd client and lease
	client *clientv3.Client
	lease  clientv3.Lease
	ID     clientv3.LeaseID
	// leaseTimeout and expireTime are used to control the lease's lifetime
	leaseTimeout time.Duration
	expireTime   atomic.Value
}

// Grant uses `lease.Grant` to initialize the lease and expireTime.
func (l *lease) Grant(leaseTimeout int64) error {
	if l == nil {
		return errs.ErrEtcdGrantLease.GenWithStackByCause("lease is nil")
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(l.client.Ctx(), requestTimeout)
	leaseResp, err := l.lease.Grant(ctx, leaseTimeout)
	cancel()
	if err != nil {
		return errs.ErrEtcdGrantLease.Wrap(err).GenWithStackByCause()
	}
	if cost := time.Since(start); cost > slowRequestTime {
		log.Warn("lease grants too slow", zap.Duration("cost", cost), zap.String("purpose", l.Purpose))
	}
	log.Info("lease granted", zap.Int64("lease-id", int64(leaseResp.ID)), zap.Int64("lease-timeout", leaseTimeout), zap.String("purpose", l.Purpose))
	l.ID = leaseResp.ID
	l.leaseTimeout = time.Duration(leaseTimeout) * time.Second
	l.expireTime.Store(start.Add(time.Duration(leaseResp.TTL) * time.Second))
	return nil
}

// Close releases the lease.
func (l *lease) Close() error {
	if l == nil {
		return nil
	}
	// Reset expire time.
	l.expireTime.Store(typeutil.ZeroTime)
	// Try to revoke lease to make subsequent elections faster.
	ctx, cancel := context.WithTimeout(l.client.Ctx(), revokeLeaseTimeout)
	defer cancel()
	l.lease.Revoke(ctx, l.ID)
	return l.lease.Close()
}

// IsExpired checks if the lease is expired. If it returns true,
// current leader should step down and try to re-elect again.
func (l *lease) IsExpired() bool {
	if l == nil || l.expireTime.Load() == nil {
		return true
	}
	return time.Now().After(l.expireTime.Load().(time.Time))
}

// KeepAlive auto renews the lease and update expireTime.
func (l *lease) KeepAlive(ctx context.Context) {
	if l == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeCh := l.keepAliveWorker(ctx, l.leaseTimeout/3)

	var maxExpire time.Time
	for {
		select {
		case t := <-timeCh:
			if t.After(maxExpire) {
				maxExpire = t
				// Check again to make sure the `expireTime` still needs to be updated.
				select {
				case <-ctx.Done():
					return
				default:
					l.expireTime.Store(t)
				}
			}
		case <-time.After(l.leaseTimeout):
			log.Info("lease timeout", zap.Time("expire", l.expireTime.Load().(time.Time)), zap.String("purpose", l.Purpose))
			return
		case <-ctx.Done():
			return
		}
	}
}

// Periodically call `lease.KeepAliveOnce` and post back latest received expire time into the channel.
func (l *lease) keepAliveWorker(ctx context.Context, interval time.Duration) <-chan time.Time {
	ch := make(chan time.Time)

	go func() {
		defer logutil.LogPanic()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Info("start lease keep alive worker", zap.Duration("interval", interval), zap.String("purpose", l.Purpose))
		defer log.Info("stop lease keep alive worker", zap.String("purpose", l.Purpose))

		for {
			go func() {
				defer logutil.LogPanic()
				start := time.Now()
				ctx1, cancel := context.WithTimeout(ctx, l.leaseTimeout)
				defer cancel()
				res, err := l.lease.KeepAliveOnce(ctx1, l.ID)
				if err != nil {
					log.Warn("lease keep alive failed", zap.String("purpose", l.Purpose), errs.ZapError(err))
					return
				}
				if res.TTL > 0 {
					expire := start.Add(time.Duration(res.TTL) * time.Second)
					select {
					case ch <- expire:
					case <-ctx1.Done():
					}
				}
			}()

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return ch
}
