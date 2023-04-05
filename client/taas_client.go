// Copyright 2023 TiKV Project Authors.
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
	"fmt"
	"math/rand"
	"sync"
	// "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type TaasCache struct {
	cacheData map[string]*pdpb.Timestamp
	cacheLock sync.Mutex
}
type taasClient struct {
	N      int
	M      int
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	option *option

	taasCache TaasCache // store latest timestamp of each taas node
	twg       sync.WaitGroup

	keyspaceID   uint32
	svcDiscovery ServiceDiscovery
	TsoStreamBuilderFactory
	// tsoAllocators defines the mapping {dc-location -> TSO allocator leader URL}
	tsoAllocators sync.Map // Store as map[string]string
	// tsoAllocServingAddrSwitchedCallback will be called when any global/local
	// tso allocator leader is switched.
	tsoAllocServingAddrSwitchedCallback []func()

	// taasDispatcher is used to dispatch different TSO requests to
	// the corresponding taas node channel.
	taasDispatcher sync.Map // Same as map[string]chan *tsoRequest
	// dc-location -> deadline
	tsDeadline sync.Map // Same as map[string]chan deadline
	// dc-location -> *lastTSO
	lastTSMap sync.Map // Same as map[string]*lastTSO

	checkTSDeadlineCh         chan struct{}
	checkTSODispatcherCh      chan struct{}
	updateTSOConnectionCtxsCh chan struct{}
}

// newTSOClient returns a new TSO client.
func newTaasClient(
	ctx context.Context, option *option, keyspaceID uint32,
	svcDiscovery ServiceDiscovery, factory TsoStreamBuilderFactory,
) *taasClient {
	ctx, cancel := context.WithCancel(ctx)
	c := &taasClient{
		ctx:    ctx,
		cancel: cancel,
		option: option,
		taasCache: TaasCache{
			cacheData: make(map[string]*pdpb.Timestamp),
		},
		keyspaceID:                keyspaceID,
		svcDiscovery:              svcDiscovery,
		TsoStreamBuilderFactory:   factory,
		checkTSDeadlineCh:         make(chan struct{}),
		checkTSODispatcherCh:      make(chan struct{}, 1),
		updateTSOConnectionCtxsCh: make(chan struct{}, 1),
	}

	eventSrc := svcDiscovery.(tsoAllocatorEventSource)
	eventSrc.SetTSOLocalServAddrsUpdatedCallback(c.updateTaasLocalServAddrs)
	eventSrc.SetTSOGlobalServAddrUpdatedCallback(c.updateTaasGlobalServAddr)
	c.svcDiscovery.AddServiceAddrsSwitchedCallback(c.scheduleUpdateTSOConnectionCtxs)

	return c
}

func (c *taasClient) Setup() {
	c.svcDiscovery.CheckMemberChanged()
	c.updateTSODispatcher()

	// Start the daemons.
	c.wg.Add(2)
	go c.tsoDispatcherCheckLoop()
	go c.tsCancelLoop()
}

// Close closes the TSO client
func (c *taasClient) Close() {
	if c == nil {
		return
	}
	log.Info("closing tso client")

	c.cancel()
	c.wg.Wait()

	log.Info("close tso client")
	c.taasDispatcher.Range(func(_, dispatcherInterface interface{}) bool {
		if dispatcherInterface != nil {
			dispatcher := dispatcherInterface.(*tsoDispatcher)
			tsoErr := errors.WithStack(errClosing)
			dispatcher.tsoBatchController.revokePendingTSORequest(tsoErr)
			dispatcher.dispatcherCancel()
		}
		return true
	})

	log.Info("tso client is closed")
}

// GetTaasAllocators returns {Taas node name -> Taas allocator URL} connection map
func (c *taasClient) GetTaasAllocators() *sync.Map {
	return &c.tsoAllocators
}

// GetTaasAllocatorServingAddrByNodeName returns the tso allocator of the given taas node name
func (c *taasClient) GetTaasAllocatorServingAddrByNodeName(nodeName string) (string, bool) {
	url, exist := c.tsoAllocators.Load(nodeName)
	if !exist {
		return "", false
	}
	return url.(string), true
}

// GetTSOAllocatorClientConnByNodeName returns the taas allocator grpc client connection
// of the given taas node name
func (c *taasClient) GetTaasAllocatorClientConnByNodeName(nodeName string) (*grpc.ClientConn, string) {
	url, ok := c.tsoAllocators.Load(nodeName)
	if !ok {
		panic(fmt.Sprintf("the allocator in %s should exist", nodeName))
	}
	cc, ok := c.svcDiscovery.GetClientConns().Load(url)
	if !ok {
		panic(fmt.Sprintf("the client connection of %s in %s should exist", url, nodeName))
	}
	return cc.(*grpc.ClientConn), url.(string)
}

// AddTSOAllocatorServingAddrSwitchedCallback adds callbacks which will be called
// when any global/local tso allocator service endpoint is switched.
func (c *taasClient) AddTSOAllocatorServingAddrSwitchedCallback(callbacks ...func()) {
	c.tsoAllocServingAddrSwitchedCallback = append(c.tsoAllocServingAddrSwitchedCallback, callbacks...)
}

/***
 * for service discovery
***/
// serveice_discovery
func (c *taasClient) updateTaasLocalServAddrs(allocatorMap map[string]string) error {
	if len(allocatorMap) == 0 {
		return nil
	}

	updated := false

	// Switch to the new one
	for nodeName, addr := range allocatorMap {
		if len(addr) == 0 {
			continue
		}
		oldAddr, exist := c.GetTaasAllocatorServingAddrByNodeName(nodeName)
		if exist && addr == oldAddr {
			continue
		}
		updated = true
		if _, err := c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			log.Warn("[tso] failed to connect dc tso allocator serving address",
				zap.String("nodeName", nodeName),
				zap.String("serving-address", addr),
				errs.ZapError(err))
			return err
		}
		c.tsoAllocators.Store(nodeName, addr)
		log.Info("[tso] switch dc tso allocator serving address",
			zap.String("nodeName", nodeName),
			zap.String("new-address", addr),
			zap.String("old-address", oldAddr))
	}

	// Garbage collection of the old TSO allocator primaries
	c.gcAllocatorServingAddr(allocatorMap)

	if updated {
		c.scheduleCheckTSODispatcher()
	}

	return nil
}

// serveice_discovery
func (c *taasClient) updateTaasGlobalServAddr(addr string) error {
	// c.tsoAllocators.Store(globalDCLocation, addr)
	log.Info("[taas] skip global tso allocator serving address",
		zap.String("dc-location", globalDCLocation),
		zap.String("new-address", addr))
	// c.scheduleCheckTSODispatcher()
	return nil
}

// serveice_discovery
func (c *taasClient) gcAllocatorServingAddr(curAllocatorMap map[string]string) {
	// Clean up the old TSO allocators
	c.tsoAllocators.Range(func(dcLocationKey, _ interface{}) bool {
		dcLocation := dcLocationKey.(string)
		// Skip the Global TSO Allocator
		if dcLocation == globalDCLocation {
			return true
		}
		if _, exist := curAllocatorMap[dcLocation]; !exist {
			log.Info("[tso] delete unused tso allocator", zap.String("dc-location", dcLocation))
			c.tsoAllocators.Delete(dcLocation)
		}
		return true
	})
}

// serveice_discovery
func (c *taasClient) backupClientConn() (*grpc.ClientConn, string) {
	addrs := c.svcDiscovery.GetBackupAddrs()
	if len(addrs) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for i := 0; i < len(addrs); i++ {
		addr := addrs[rand.Intn(len(addrs))]
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return cc, addr
		}
	}
	return nil, ""
}
