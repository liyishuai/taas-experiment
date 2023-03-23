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

package install

import (
	"github.com/tikv/pd/pkg/mcs/registry"
	rm_server "github.com/tikv/pd/pkg/mcs/resource_manager/server"

	// init API group
	_ "github.com/tikv/pd/pkg/mcs/resource_manager/server/apis/v1"
)

func init() {
	Install(registry.ServerServiceRegistry)
}

// Install registers the API group and grpc service.
func Install(register *registry.ServiceRegistry) {
	register.RegisterService("ResourceManager", rm_server.NewService[*rm_server.Server])
}
