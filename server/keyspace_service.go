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

package server

import (
	"context"
	"path"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/keyspace"
	"go.etcd.io/etcd/clientv3"
)

// KeyspaceServer wraps GrpcServer to provide keyspace service.
type KeyspaceServer struct {
	*GrpcServer
}

// getErrorHeader returns corresponding ResponseHeader based on err.
func (s *KeyspaceServer) getErrorHeader(err error) *pdpb.ResponseHeader {
	switch err {
	case keyspace.ErrKeyspaceExists:
		return s.wrapErrorToHeader(pdpb.ErrorType_DUPLICATED_ENTRY, err.Error())
	case keyspace.ErrKeyspaceNotFound:
		return s.wrapErrorToHeader(pdpb.ErrorType_ENTRY_NOT_FOUND, err.Error())
	default:
		return s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error())
	}
}

// LoadKeyspace load and return target keyspace metadata.
// Request must specify keyspace name.
// On Error, keyspaceMeta in response will be nil,
// error information will be encoded in response header with corresponding error type.
func (s *KeyspaceServer) LoadKeyspace(_ context.Context, request *keyspacepb.LoadKeyspaceRequest) (*keyspacepb.LoadKeyspaceResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &keyspacepb.LoadKeyspaceResponse{Header: s.notBootstrappedHeader()}, nil
	}

	manager := s.GetKeyspaceManager()
	meta, err := manager.LoadKeyspace(request.GetName())
	if err != nil {
		return &keyspacepb.LoadKeyspaceResponse{Header: s.getErrorHeader(err)}, nil
	}
	return &keyspacepb.LoadKeyspaceResponse{
		Header:   s.header(),
		Keyspace: meta,
	}, nil
}

// WatchKeyspaces captures and sends keyspace metadata changes to the client via gRPC stream.
// Note: It sends all existing keyspaces as it's first package to the client.
func (s *KeyspaceServer) WatchKeyspaces(request *keyspacepb.WatchKeyspacesRequest, stream keyspacepb.Keyspace_WatchKeyspacesServer) error {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return stream.Send(&keyspacepb.WatchKeyspacesResponse{Header: s.notBootstrappedHeader()})
	}

	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()

	err := s.sendAllKeyspaceMeta(ctx, stream)
	if err != nil {
		return err
	}
	watchChan := s.client.Watch(ctx, path.Join(s.rootPath, endpoint.KeyspaceMetaPrefix()), clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			keyspaces := make([]*keyspacepb.KeyspaceMeta, 0, len(res.Events))
			for _, event := range res.Events {
				if event.Type != clientv3.EventTypePut {
					continue
				}
				meta := &keyspacepb.KeyspaceMeta{}
				if err = proto.Unmarshal(event.Kv.Value, meta); err != nil {
					return err
				}
				keyspaces = append(keyspaces, meta)
			}
			if len(keyspaces) > 0 {
				if err = stream.Send(&keyspacepb.WatchKeyspacesResponse{Header: s.header(), Keyspaces: keyspaces}); err != nil {
					return err
				}
			}
		}
	}
}

func (s *KeyspaceServer) sendAllKeyspaceMeta(ctx context.Context, stream keyspacepb.Keyspace_WatchKeyspacesServer) error {
	getResp, err := s.client.Get(ctx, path.Join(s.rootPath, endpoint.KeyspaceMetaPrefix()), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	metas := make([]*keyspacepb.KeyspaceMeta, getResp.Count)
	for i, kv := range getResp.Kvs {
		meta := &keyspacepb.KeyspaceMeta{}
		if err = proto.Unmarshal(kv.Value, meta); err != nil {
			return err
		}
		metas[i] = meta
	}
	return stream.Send(&keyspacepb.WatchKeyspacesResponse{Header: s.header(), Keyspaces: metas})
}

// UpdateKeyspaceState updates the state of keyspace specified in the request.
func (s *KeyspaceServer) UpdateKeyspaceState(_ context.Context, request *keyspacepb.UpdateKeyspaceStateRequest) (*keyspacepb.UpdateKeyspaceStateResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &keyspacepb.UpdateKeyspaceStateResponse{Header: s.notBootstrappedHeader()}, nil
	}
	manager := s.GetKeyspaceManager()
	meta, err := manager.UpdateKeyspaceStateByID(request.GetId(), request.GetState(), time.Now().Unix())
	if err != nil {
		return &keyspacepb.UpdateKeyspaceStateResponse{Header: s.getErrorHeader(err)}, nil
	}
	return &keyspacepb.UpdateKeyspaceStateResponse{
		Header:   s.header(),
		Keyspace: meta,
	}, nil
}
