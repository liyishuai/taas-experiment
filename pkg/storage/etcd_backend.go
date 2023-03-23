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

package storage

import (
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"go.etcd.io/etcd/clientv3"
)

// etcdBackend is a storage backend that stores data in etcd,
// which is mainly used by the PD server.
type etcdBackend struct {
	*endpoint.StorageEndpoint
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdBackend(client *clientv3.Client, rootPath string) *etcdBackend {
	return &etcdBackend{
		endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(client, rootPath),
			nil,
		),
	}
}
