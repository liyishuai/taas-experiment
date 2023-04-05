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
	//"fmt"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	// "go.uber.org/zap"

	// "github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"google.golang.org/grpc"
)

// TSO Stream Builder Factory

type TsoStreamBuilderFactory interface {
	makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder
}

type pdTSOStreamBuilderFactory struct{}

func (f *pdTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &pdTSOStreamBuilder{client: pdpb.NewPDClient(cc)}
}

type tsoTSOStreamBuilderFactory struct{}

func (f *tsoTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &tsoTSOStreamBuilder{client: tsopb.NewTSOClient(cc)}
}

// TSO Stream Builder

type tsoStreamBuilder interface {
	build(context.Context, context.CancelFunc, time.Duration) (tsoStream, error)
	buildTaas(context.Context, context.CancelFunc, time.Duration) (tsoStream, error)
}

type pdTSOStreamBuilder struct {
	client pdpb.PDClient
}

func (b *pdTSOStreamBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return &pdTSOStream{stream: stream}, nil
	}
	return nil, err
}
func (b *pdTSOStreamBuilder) buildTaas(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (tsoStream, error){
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Taas(ctx)
	done <- struct{}{}
	if err == nil {
		return &pdTaasStream{stream: stream}, nil
	}
	return nil, err
	return nil, nil

	//fmt.Println("run my pdTSOStreamBuilder!!!!!!!")
	return nil, nil
}

type tsoTSOStreamBuilder struct {
	client tsopb.TSOClient
}

func (b *tsoTSOStreamBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return &tsoTSOStream{stream: stream}, nil
	}
	return nil, err
}
func (b *tsoTSOStreamBuilder) buildTaas(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Taas(ctx)
	done <- struct{}{}
	if err == nil {
		return &taasTSOStream{stream: stream}, nil
	}
	return nil,err
}

func checkStreamTimeout(ctx context.Context, cancel context.CancelFunc, done chan struct{}, timeout time.Duration) {
	select {
	case <-done:
		return
	case <-time.After(timeout):
		cancel()
	case <-ctx.Done():
	}
	<-done
}

// TSO Stream

type tsoStream interface {
	// processRequests processes TSO requests in streaming mode to get timestamps
	processRequests(clusterID uint64, dcLocation string, requests []*tsoRequest,
		batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error)
}

type pdTSOStream struct {
	stream pdpb.PD_TsoClient
}

func (s *pdTSOStream) processRequests(clusterID uint64, dcLocation string, requests []*tsoRequest,
	batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error) {
	start := time.Now()
	count := int64(len(requests))
	req := &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}
	if err = s.stream.Send(req); err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	tsoBatchSendLatency.Observe(float64(time.Since(batchStartTime)))
	resp, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return
	}

	physical, logical, suffixBits = resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()

	return
}

type tsoTSOStream struct {
	stream tsopb.TSO_TsoClient
}


func (s *tsoTSOStream) processRequests(clusterID uint64, dcLocation string, requests []*tsoRequest,
	batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error) {
	start := time.Now()
	count := int64(len(requests))
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId: clusterID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}

	if err = s.stream.Send(req); err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	tsoBatchSendLatency.Observe(float64(time.Since(batchStartTime)))
	resp, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return
	}

	physical, logical, suffixBits = resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
	return
}

type pdTaasStream struct {
	stream pdpb.PD_TaasClient
}

func (s *pdTaasStream) processRequests(clusterID uint64, nodeName string, requests []*tsoRequest,
	batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error) {
		reqLowerBound := int64(0)
		for _, req := range(requests) {
			if req.physical > reqLowerBound {
				reqLowerBound = req.physical
			}
		}
		start := time.Now()
		count := int64(len(requests))
		req := &pdpb.TaasRequest{
			Header: &pdpb.RequestHeader{
				ClusterId: clusterID,
			},
			Timestamp: &pdpb.Timestamp{
				Logical: int64(0),
				Physical: reqLowerBound,
				SuffixBits:uint32(0),
			},
			Count:      uint32(1),
			DcLocation: taasDCLocation,
		}
		if err = s.stream.Send(req); err != nil {
			if err == io.EOF {
				err = errs.ErrClientTSOStreamClosed
			} else {
				err = errors.WithStack(err)
			}
			return
		}
		tsoBatchSendLatency.Observe(float64(time.Since(batchStartTime)))
		resp, err := s.stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = errs.ErrClientTSOStreamClosed
			} else {
				err = errors.WithStack(err)
			}
			return
		}
		requestDurationTSO.Observe(time.Since(start).Seconds())
		tsoBatchSize.Observe(float64(count))
	
		if resp.GetCount() != uint32(count) {
			// log.Error("zghtag", zap.Uint32("taasTSOStream", resp.GetCount()))
			// err = errors.WithStack(errTSOLength)
			// return
		}
	
		physical, logical, suffixBits = resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
		// log.Info("zghtag: use pdTaasStream", zap.Int64("taasTSOStream", physical))
		return 

}

type taasTSOStream struct {
	stream tsopb.TSO_TaasClient
}
func (s *taasTSOStream) processRequests(clusterID uint64, dcLocation string, requests []*tsoRequest,
	batchStartTime time.Time) (physical, logical int64, suffixBits uint32, err error) {
		log.Info("zghtag: use taasTSOStream")
   		return 0,0,0,nil
}