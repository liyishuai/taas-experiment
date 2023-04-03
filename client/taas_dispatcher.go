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
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	// "github.com/tikv/pd/client/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	// "google.golang.org/grpc/status"
)

type taasDispatcher struct {
	dispatcherCancel   context.CancelFunc
	tsoBatchController *tsoBatchController
}

const (

)

func (c *taasClient) scheduleCheckTSODispatcher() {
	select {
	case c.checkTSODispatcherCh <- struct{}{}:
	default:
	}
}

func (c *taasClient) scheduleUpdateTSOConnectionCtxs() {
	select {
	case c.updateTSOConnectionCtxsCh <- struct{}{}:
	default:
	}
}

// TODO:zgh dispatch to all taas node here
func (c *taasClient) dispatchRequest(dcLocation string, request *tsoRequest) error {
	// put request into stream of each taas rpc server
	if dcLocation != taasDCLocation{
		log.Error("zghtag", zap.String("wrong DC", dcLocation))
	}
	c.taasDispatcher.Range(func(nodeNameKey, _ interface{}) bool {
		nodeName := nodeNameKey.(string)
		// log.Info("zghtag", zap.String("tsoClientCreate", dcLocation))
		if !c.checkTaasDispatcher(nodeName) {
			c.createTaasDispatcher(nodeName)
		}
		return true
	})
	dispatcher, ok := c.taasDispatcher.Load("pd1")
	// log.Info("zghtag", zap.String("taas client dispatchRequest", dcLocation))
	if !ok {
		err := errs.ErrClientGetTSO.FastGenByArgs(fmt.Sprintf("unknown dc-location %s to the client", dcLocation))
		log.Error("[tso] dispatch tso request error", zap.String("dc-location", dcLocation), errs.ZapError(err))
		c.svcDiscovery.ScheduleCheckMemberChanged()
		return err
	}
	dispatcher.(*taasDispatcher).tsoBatchController.tsoRequestCh <- request
	return nil
}


func (c *taasClient) updateTSODispatcher() {
	// Set up the new TSO dispatcher and batch controller.
	c.GetTaasAllocators().Range(func(nodeNameKey, _ interface{}) bool {
		nodeName := nodeNameKey.(string)
		log.Info("zghtag", zap.String("tsoClientCreate nodeName", nodeName))
		if nodeNameKey != globalDCLocation && !c.checkTaasDispatcher(nodeName) {
			c.createTaasDispatcher(nodeName)
		}
		return true
	})
	// Clean up the unused TSO dispatcher
	c.taasDispatcher.Range(func(nodeNameKey, dispatcher interface{}) bool {
		nodeName := nodeNameKey.(string)
		// Skip the Global TSO Allocator
		if _, exist := c.GetTaasAllocators().Load(nodeName); !exist {
			log.Info("[tso] delete unused tso dispatcher", zap.String("nodeName", nodeName))
			dispatcher.(*taasDispatcher).dispatcherCancel()
			c.taasDispatcher.Delete(nodeName)
		}
		return true
	})
}


func (c *taasClient) tsCancelLoop() {
	defer c.wg.Done()

	tsCancelLoopCtx, tsCancelLoopCancel := context.WithCancel(c.ctx)
	defer tsCancelLoopCancel()

	ticker := time.NewTicker(tsLoopDCCheckInterval)
	defer ticker.Stop()
	for {
		// Watch every tsDeadlineCh of taas allocator
		c.GetTaasAllocators().Range(func(dcLocation, _ interface{}) bool {
			c.watchTSDeadline(tsCancelLoopCtx, dcLocation.(string))
			return true
		})
		select {
		case <-c.checkTSDeadlineCh:
			continue
		case <-ticker.C:
			continue
		case <-tsCancelLoopCtx.Done():
			log.Info("exit tso requests cancel loop")
			return
		}
	}
}

func (c *taasClient) watchTSDeadline(ctx context.Context, dcLocation string) {
	if _, exist := c.tsDeadline.Load(dcLocation); !exist {
		tsDeadlineCh := make(chan deadline, 1)
		c.tsDeadline.Store(dcLocation, tsDeadlineCh)
		go func(dc string, tsDeadlineCh <-chan deadline) {
			for {
				select {
				case d := <-tsDeadlineCh:
					select {
					case <-d.timer:
						log.Error("[tso] tso request is canceled due to timeout", zap.String("dc-location", dc), errs.ZapError(errs.ErrClientGetTSOTimeout))
						d.cancel()
					case <-d.done:
						continue
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(dcLocation, tsDeadlineCh)
	}
}

func (c *taasClient) scheduleCheckTSDeadline() {
	select {
	case c.checkTSDeadlineCh <- struct{}{}:
	default:
	}
}

func (c *taasClient) tsoDispatcherCheckLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	ticker := time.NewTicker(tsLoopDCCheckInterval)
	defer ticker.Stop()
	for {
		c.updateTSODispatcher()
		select {
		case <-ticker.C:
		case <-c.checkTSODispatcherCh:
		case <-loopCtx.Done():
			log.Info("exit taas dispatcher loop")
			return
		}
	}
}

func (c *taasClient) checkAllocator(
	dispatcherCtx context.Context,
	forwardCancel context.CancelFunc,
	dc, forwardedHostTrim, addrTrim, url string,
	updateAndClear func(newAddr string, connectionCtx *taasConnectionContext)) {
	defer func() {
		// cancel the forward stream
		forwardCancel()
		requestForwarded.WithLabelValues(forwardedHostTrim, addrTrim).Set(0)
	}()
	log.Info("zghtag", zap.String("taas allocator", dc))
	cc, u := c.GetTaasAllocatorClientConnByNodeName(dc)
	healthCli := healthpb.NewHealthClient(cc)
	for {
		// the pd/allocator leader change, we need to re-establish the stream
		if u != url {
			log.Info("[tso] the leader of the allocator leader is changed", zap.String("dc", dc), zap.String("origin", url), zap.String("new", u))
			return
		}
		healthCtx, healthCancel := context.WithTimeout(dispatcherCtx, c.option.timeout)
		resp, err := healthCli.Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		failpoint.Inject("unreachableNetwork", func() {
			resp.Status = healthpb.HealthCheckResponse_UNKNOWN
		})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			// create a stream of the original allocator
			cctx, cancel := context.WithCancel(dispatcherCtx)
			stream, err := c.TsoStreamBuilderFactory.makeBuilder(cc).build(cctx, cancel, c.option.timeout)
			taasStream, err := c.TsoStreamBuilderFactory.makeBuilder(cc).buildTaas(cctx, cancel, c.option.timeout)
			if err != nil {
				log.Error("[tso] build taas stream failed")
			}
			if err == nil && stream != nil {
				log.Info("[tso] recover the original tso stream since the network has become normal", zap.String("dc", dc), zap.String("url", url))
				updateAndClear(url, &taasConnectionContext{url, taasStream, cctx, cancel})
				return
			}
		}
		select {
		case <-dispatcherCtx.Done():
			return
		case <-time.After(time.Second):
			// To ensure we can get the latest allocator leader
			// and once the leader is changed, we can exit this function.
			_, u = c.GetTaasAllocatorClientConnByNodeName(dc)
		}
	}
}

func (c *taasClient) checkTaasDispatcher(nodeName string) bool {
	dispatcher, ok := c.taasDispatcher.Load(nodeName)
	if !ok || dispatcher == nil {
		// log.Error("zghtag", zap.String("can't get dispatcher", nodeName))
		return false
	}
	return true
}

func (c *taasClient) createTaasDispatcher(nodeName string) {
	dispatcherCtx, dispatcherCancel := context.WithCancel(c.ctx)
	dispatcher := &taasDispatcher{
		dispatcherCancel: dispatcherCancel,
		tsoBatchController: newTSOBatchController(
			make(chan *tsoRequest, defaultMaxTSOBatchSize*2),
			defaultMaxTSOBatchSize),
	}

	if _, ok := c.taasDispatcher.LoadOrStore(nodeName, dispatcher); !ok {
		// Successfully stored the value. Start the following goroutine.
		// Each goroutine is responsible for handling the tso stream request for its dc-location.
		// The only case that will make the dispatcher goroutine exit
		// is that the loopCtx is done, otherwise there is no circumstance
		// this goroutine should exit.
		c.wg.Add(1)
		go c.handleDispatcher(dispatcherCtx, nodeName, dispatcher.tsoBatchController)
		log.Info("[taas] tso handle dispatcher", zap.String("taas node name", nodeName))
	} else {
		dispatcherCancel()
	}
}

func (c *taasClient) handleDispatcher(
	dispatcherCtx context.Context,
	nodeName string,
	tbc *tsoBatchController) {
	var (
		err        error
		streamAddr string
		stream     tsoStream
		// streamMap  sync.Map
		streamCtx  context.Context
		cancel     context.CancelFunc
		// addr -> connectionContext
		connectionCtxs sync.Map
		opts           []opentracing.StartSpanOption
		nodeAddr   string
	)
	defer func() {
		log.Info("[taas] exit taas dispatcher", zap.String("dc-location", nodeName))
		// Cancel all connections.
		connectionCtxs.Range(func(_, cc interface{}) bool {
			cc.(*taasConnectionContext).cancel()
			return true
		})
		c.wg.Done()
	}()
	// Call updateTSOConnectionCtxs once to init the connectionCtxs first.
	c.updateTSOConnectionCtxs(dispatcherCtx, nodeName, &connectionCtxs)

	// Loop through each batch of TSO requests and send them for processing.
	streamLoopTimer := time.NewTimer(c.option.timeout)

	// Store taas nodeAddr of nodeName
	nodeAddr, ok := c.GetTaasAllocatorServingAddrByNodeName(nodeName)
	if !ok {
		log.Error("[taas] get taas address by nodename failed", zap.String("nodeName", nodeName))
	}

tsoBatchLoop:
	for {
		select {
		case <-dispatcherCtx.Done():
			return
		default:
		}
		// Start to collect the TSO requests.
		maxBatchWaitInterval := c.option.getMaxTSOBatchWaitInterval()
		if err = tbc.fetchPendingRequests(dispatcherCtx, maxBatchWaitInterval); err != nil {
			if err == context.Canceled {
				log.Info("[taas] stop fetching the pending tso requests due to context canceled",
					zap.String("nodeName", nodeName))
			} else {
				log.Error("[taas] fetch pending tso requests error",
					zap.String("nodeName", nodeName), errs.ZapError(errs.ErrClientGetTSO, err))
			}
			return
		}
		if maxBatchWaitInterval >= 0 {
			tbc.adjustBestBatchSize()
		}
		streamLoopTimer.Reset(c.option.timeout)
		// Choose a stream to send the TSO gRPC request.
	streamChoosingLoop:
		for {
			// connectionCtx := c.chooseStream(&connectionCtxs)
			// connectionCtx := &taasConnectionContext{}
			ctx, ok := connectionCtxs.Load(nodeAddr)
			if !ok {
				log.Error("[taas] load taas stream failed", zap.String("nodeAddr", nodeAddr))
			}
			connectionCtx := ctx.(*taasConnectionContext)

			if connectionCtx != nil {
				streamAddr, stream, streamCtx, cancel = connectionCtx.streamAddr, connectionCtx.taasStream, connectionCtx.ctx, connectionCtx.cancel
			}
			// Check stream and retry if necessary.
			if stream == nil {
				log.Info("[taas] tso stream is not ready", zap.String("nodeName", nodeName))
				if c.updateTSOConnectionCtxs(dispatcherCtx, nodeName, &connectionCtxs) {
					continue streamChoosingLoop
				}
				select {
				case <-dispatcherCtx.Done():
					return
				case <-streamLoopTimer.C:
					err = errs.ErrClientCreateTSOStream.FastGenByArgs(errs.RetryTimeoutErr)
					log.Error("[tso] create tso stream error", zap.String("nodeName", nodeName), errs.ZapError(err))
					c.svcDiscovery.ScheduleCheckMemberChanged()
					c.finishTSORequest(tbc.getCollectedRequests(), 0, 0, 0, errors.WithStack(err))
					continue tsoBatchLoop
				case <-time.After(retryInterval):
					continue streamChoosingLoop
				}
			}
			select {
			case <-streamCtx.Done():
				log.Info("[taas] tso stream is canceled", zap.String("nodeName", nodeName), zap.String("stream-addr", streamAddr))
				// Set `stream` to nil and remove this stream from the `connectionCtxs` due to being canceled.
				connectionCtxs.Delete(streamAddr)
				cancel()
				stream = nil
				continue
			default:
				break streamChoosingLoop
			}
		}
		done := make(chan struct{})
		dl := deadline{
			timer:  time.After(c.option.timeout),
			done:   done,
			cancel: cancel,
		}
		tsDeadlineCh, ok := c.tsDeadline.Load(nodeName)
		for !ok || tsDeadlineCh == nil {
			c.scheduleCheckTSDeadline()
			time.Sleep(time.Millisecond * 100)
			tsDeadlineCh, ok = c.tsDeadline.Load(nodeName)
		}
		select {
		case <-dispatcherCtx.Done():
			return
		case tsDeadlineCh.(chan deadline) <- dl:
		}
		opts = extractSpanReference(tbc, opts[:0])
		err = c.processTaasRequests(stream, taasDCLocation, tbc, opts)
		close(done)
		// If error happens during tso stream handling, reset stream and run the next trial.
		if err != nil {
			select {
			case <-dispatcherCtx.Done():
				return
			default:
			}
			c.svcDiscovery.ScheduleCheckMemberChanged()
			log.Error("[tso] getTS error", zap.String("dc-location", nodeName), zap.String("stream-addr", streamAddr), errs.ZapError(errs.ErrClientGetTSO, err))
			// Set `stream` to nil and remove this stream from the `connectionCtxs` due to error.
			connectionCtxs.Delete(streamAddr)
			cancel()
			stream = nil
			// Because ScheduleCheckMemberChanged is asynchronous, if the leader changes, we better call `updateMember` ASAP.
			if IsLeaderChange(err) {
				if err := c.svcDiscovery.CheckMemberChanged(); err != nil {
					select {
					case <-dispatcherCtx.Done():
						return
					default:
					}
				}
				// Because the TSO Follower Proxy could be configured online,
				// If we change it from on -> off, background updateTSOConnectionCtxs
				// will cancel the current stream, then the EOF error caused by cancel()
				// should not trigger the updateTSOConnectionCtxs here.
				// So we should only call it when the leader changes.
				c.updateTSOConnectionCtxs(dispatcherCtx, nodeName, &connectionCtxs)
			}
		}
	}
}

type taasConnectionContext struct {
	streamAddr string
	// Current stream to send gRPC requests, pdpb.PD_TsoClient for a leader/follower in the PD cluser,
	// or tsopb.TSO_TsoClient for a primary/secondary in the TSO clusrer
	taasStream tsoStream
	ctx        context.Context
	cancel     context.CancelFunc
}

func (c *taasClient) updateTSOConnectionCtxs(updaterCtx context.Context, dc string, connectionCtxs *sync.Map) bool {
	// Normal connection creating, it will be affected by the `enableForwarding`.
	createTSOConnection := c.tryConnectToTaas
	if err := createTSOConnection(updaterCtx, dc, connectionCtxs); err != nil {
		log.Error("[tso] update connection contexts failed", zap.String("dc", dc), errs.ZapError(err))
		return false
	}
	return true
}

// getAllTSOStreamBuilders returns a TSO stream builder for every service endpoint of TSO leader/followers
// or of keyspace group primary/secondaries.
func (c *taasClient) getAllTSOStreamBuilders() map[string]tsoStreamBuilder {
	var (
		addrs          = c.svcDiscovery.GetURLs()
		streamBuilders = make(map[string]tsoStreamBuilder, len(addrs))
		cc             *grpc.ClientConn
		err            error
	)
	for _, addr := range addrs {
		if len(addrs) == 0 {
			continue
		}
		if cc, err = c.svcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			streamBuilders[addr] = c.TsoStreamBuilderFactory.makeBuilder(cc)
		}
	}
	return streamBuilders
}

// tryConnectToTSOWithProxy will create multiple streams to all the service endpoints to work as
// a TSO proxy to reduce the pressure of the main serving service endpoint.
func (c *taasClient) tryConnectToTaas(dispatcherCtx context.Context, dc string, connectionCtxs *sync.Map) error {
	tsoStreamBuilders := c.getAllTSOStreamBuilders()

	// GC the stale one.
	connectionCtxs.Range(func(addr, cc interface{}) bool {
		if _, ok := tsoStreamBuilders[addr.(string)]; !ok {
			cc.(*taasConnectionContext).cancel()
			connectionCtxs.Delete(addr)
		}
		return true
	})
	// Update the missing one.
	for addr, tsoStreamBuilder := range tsoStreamBuilders {
		//log.Info("zghtag", zap.String("tsoStreamBuilder", addr))
		if _, ok := connectionCtxs.Load(addr); ok {
			continue
		}
		cctx, cancel := context.WithCancel(dispatcherCtx)
		// if dc != taasDCLocation {
		// 	log.Error("[taas] not taas stream builder", zap.String("dc", dc), zap.String("addr", addr))
		// }
		// Create the TSO stream.
		taasStream, err := tsoStreamBuilder.buildTaas(cctx, cancel, c.option.timeout)
		if err != nil {
			log.Error("[taas] create taas stream failed", zap.String("dc", dc), zap.String("addr", addr))
		}
		connectionCtxs.Store(addr, &taasConnectionContext{addr, taasStream, cctx, cancel})
		continue
		log.Error("[tso] create the tso stream failed", zap.String("dc", dc), zap.String("addr", addr), errs.ZapError(err))
		cancel()
	}
	return nil
}

// func extractSpanReference(tbc *tsoBatchController, opts []opentracing.StartSpanOption) []opentracing.StartSpanOption {
// 	for _, req := range tbc.getCollectedRequests() {
// 		if span := opentracing.SpanFromContext(req.requestCtx); span != nil {
// 			opts = append(opts, opentracing.ChildOf(span.Context()))
// 		}
// 	}
// 	return opts
// }

func (c *taasClient) processTaasRequests(stream tsoStream, dcLocation string, tbc *tsoBatchController, opts []opentracing.StartSpanOption) error {
	if len(opts) > 0 {
		span := opentracing.StartSpan("pdclient.processTSORequests", opts...)
		defer span.Finish()
	}

	requests := tbc.getCollectedRequests()
	count := int64(len(requests))
	physical, logical, suffixBits, err := stream.processRequests(c.svcDiscovery.GetClusterID(), dcLocation, requests, tbc.batchStartTime)
	if err != nil {
		c.finishTSORequest(requests, 0, 0, 0, err)
		return err
	}
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	firstLogical := addLogical(logical, -count+1, suffixBits)
	c.compareAndSwapTS(dcLocation, physical, firstLogical, suffixBits, count)
	c.finishTSORequest(requests, physical, firstLogical, suffixBits, nil)
	return nil
}

func (c *taasClient) compareAndSwapTS(dcLocation string, physical, firstLogical int64, suffixBits uint32, count int64) {
	largestLogical := addLogical(firstLogical, count-1, suffixBits)
	lastTSOInterface, loaded := c.lastTSMap.LoadOrStore(dcLocation, &lastTSO{
		physical: physical,
		// Save the largest logical part here
		logical: largestLogical,
	})
	if !loaded {
		return
	}
	lastTSOPointer := lastTSOInterface.(*lastTSO)
	lastPhysical := lastTSOPointer.physical
	lastLogical := lastTSOPointer.logical
	// The TSO we get is a range like [largestLogical-count+1, largestLogical], so we save the last TSO's largest logical to compare with the new TSO's first logical.
	// For example, if we have a TSO resp with logical 10, count 5, then all TSOs we get will be [6, 7, 8, 9, 10].
	if tsLessEqual(physical, firstLogical, lastPhysical, lastLogical) {
		panic(errors.Errorf("%s timestamp fallback, newly acquired ts (%d, %d) is less or equal to last one (%d, %d)",
			dcLocation, physical, firstLogical, lastPhysical, lastLogical))
	}
	lastTSOPointer.physical = physical
	// Same as above, we save the largest logical part here.
	lastTSOPointer.logical = largestLogical
}

func (c *taasClient) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32, err error) {
	for i := 0; i < len(requests); i++ {
		if span := opentracing.SpanFromContext(requests[i].requestCtx); span != nil {
			span.Finish()
		}
		requests[i].physical, requests[i].logical = physical, addLogical(firstLogical, int64(i), suffixBits)
		requests[i].done <- err
	}
}
