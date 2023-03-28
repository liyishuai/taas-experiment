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

package pd

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/grpcutil"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/keyspacepb"
	"go.uber.org/zap"
)

// KeyspaceClient manages keyspace metadata.
type KeyspaceClient interface {
	// LoadKeyspace load and return target keyspace's metadata.
	LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error)
	// WatchKeyspaces watches keyspace meta changes.
	WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error)
	// UpdateKeyspaceState updates target keyspace's state.
	UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (*keyspacepb.KeyspaceMeta, error)
}

// keyspaceClient returns the KeyspaceClient from current PD leader.
func (c *client) keyspaceClient() keyspacepb.KeyspaceClient {
	if client := c.pdSvcDiscovery.GetServingEndpointClientConn(); client != nil {
		return keyspacepb.NewKeyspaceClient(client)
	}
	return nil
}

// LoadKeyspace loads and returns target keyspace's metadata.
func (c *client) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("keyspaceClient.LoadKeyspace", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationLoadKeyspace.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &keyspacepb.LoadKeyspaceRequest{
		Header: c.requestHeader(),
		Name:   name,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.keyspaceClient().LoadKeyspace(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationLoadKeyspace.Observe(time.Since(start).Seconds())
		c.pdSvcDiscovery.ScheduleCheckMemberChanged()
		return nil, err
	}

	if resp.Header.GetError() != nil {
		cmdFailedDurationLoadKeyspace.Observe(time.Since(start).Seconds())
		return nil, errors.Errorf("Load keyspace %s failed: %s", name, resp.Header.GetError().String())
	}

	return resp.Keyspace, nil
}

// WatchKeyspaces watches keyspace meta changes.
// It returns a stream of slices of keyspace metadata.
// The first message in stream contains all current keyspaceMeta,
// all subsequent messages contains new put events for all keyspaces.
func (c *client) WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error) {
	keyspaceWatcherChan := make(chan []*keyspacepb.KeyspaceMeta)
	req := &keyspacepb.WatchKeyspacesRequest{
		Header: c.requestHeader(),
	}
	stream, err := c.keyspaceClient().WatchKeyspaces(ctx, req)
	if err != nil {
		close(keyspaceWatcherChan)
		return nil, err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("[pd] panic in keyspace client `WatchKeyspaces`", zap.Any("error", r))
				return
			}
		}()
		for {
			select {
			case <-ctx.Done():
				close(keyspaceWatcherChan)
				return
			default:
				resp, err := stream.Recv()
				if err != nil {
					return
				}
				keyspaceWatcherChan <- resp.Keyspaces
			}
		}
	}()
	return keyspaceWatcherChan, err
}

// UpdateKeyspaceState attempts to update the keyspace specified by ID to the target state,
// it will also record StateChangedAt for the given keyspace if a state change took place.
// Currently, legal operations includes:
//
//	ENABLED -> {ENABLED, DISABLED}
//	DISABLED -> {ENABLED, DISABLED, ARCHIVED}
//	ARCHIVED -> {ARCHIVED, TOMBSTONE}
//	TOMBSTONE -> {TOMBSTONE}
//
// Updated keyspace meta will be returned.
func (c *client) UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (*keyspacepb.KeyspaceMeta, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("keyspaceClient.UpdateKeyspaceState", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateKeyspaceState.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &keyspacepb.UpdateKeyspaceStateRequest{
		Header: c.requestHeader(),
		Id:     id,
		State:  state,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.keyspaceClient().UpdateKeyspaceState(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationUpdateKeyspaceState.Observe(time.Since(start).Seconds())
		c.pdSvcDiscovery.ScheduleCheckMemberChanged()
		return nil, err
	}

	if resp.Header.GetError() != nil {
		cmdFailedDurationUpdateKeyspaceState.Observe(time.Since(start).Seconds())
		return nil, errors.Errorf("Update state for keyspace id %d failed: %s", id, resp.Header.GetError().String())
	}

	return resp.Keyspace, nil
}
