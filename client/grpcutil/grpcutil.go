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

package grpcutil

import (
	"context"
	"crypto/tls"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/tlsutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	dialTimeout = 3 * time.Second
	// ForwardMetadataKey is used to record the forwarded host of PD.
	ForwardMetadataKey = "pd-forwarded-host"
)

// GetClientConn returns a gRPC client connection.
// creates a client connection to the given target. By default, it's
// a non-blocking dial (the function won't wait for connections to be
// established, and connecting happens in the background). To make it a blocking
// dial, use WithBlock() dial option.
//
// In the non-blocking case, the ctx does not act against the connection. It
// only controls the setup steps.
//
// In the blocking case, ctx can be used to cancel or expire the pending
// connection. Once this function returns, the cancellation and expiration of
// ctx will be noop. Users should call ClientConn.Close to terminate all the
// pending operations after this function returns.
func GetClientConn(ctx context.Context, addr string, tlsCfg *tls.Config, do ...grpc.DialOption) (*grpc.ClientConn, error) {
	opt := grpc.WithInsecure() //nolint
	if tlsCfg != nil {
		creds := credentials.NewTLS(tlsCfg)
		opt = grpc.WithTransportCredentials(creds)
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errs.ErrURLParse.Wrap(err).GenWithStackByCause()
	}
	cc, err := grpc.DialContext(ctx, u.Host, append(do, opt)...)
	if err != nil {
		return nil, errs.ErrGRPCDial.Wrap(err).GenWithStackByCause()
	}
	return cc, nil
}

// BuildForwardContext creates a context with receiver metadata information.
// It is used in client side.
func BuildForwardContext(ctx context.Context, addr string) context.Context {
	md := metadata.Pairs(ForwardMetadataKey, addr)
	return metadata.NewOutgoingContext(ctx, md)
}

// GetOrCreateGRPCConn returns the corresponding grpc client connection of the given addr.
// Returns the old one if's already existed in the clientConns; otherwise creates a new one and returns it.
func GetOrCreateGRPCConn(ctx context.Context, clientConns *sync.Map, addr string, tlsCfg *tlsutil.TLSConfig, opt ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, ok := clientConns.Load(addr)
	if ok {
		return conn.(*grpc.ClientConn), nil
	}
	tlsConfig, err := tlsCfg.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	dCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	cc, err := GetClientConn(dCtx, addr, tlsConfig, opt...)
	if err != nil {
		return nil, err
	}
	old, ok := clientConns.LoadOrStore(addr, cc)
	if !ok {
		// Successfully stored the connection.
		return cc, nil
	}
	cc.Close()
	log.Debug("use old connection", zap.String("target", cc.Target()), zap.String("state", cc.GetState().String()))
	return old.(*grpc.ClientConn), nil
}
