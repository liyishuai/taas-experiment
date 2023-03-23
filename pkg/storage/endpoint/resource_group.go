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

package endpoint

import (
	"github.com/gogo/protobuf/proto"
)

// ResourceGroupStorage defines the storage operations on the resource group.
type ResourceGroupStorage interface {
	LoadResourceGroupSettings(f func(k, v string)) error
	SaveResourceGroupSetting(name string, msg proto.Message) error
	DeleteResourceGroupSetting(name string) error
	LoadResourceGroupStates(f func(k, v string)) error
	SaveResourceGroupStates(name string, obj interface{}) error
	DeleteResourceGroupStates(name string) error
	SaveControllerConfig(config interface{}) error
}

var _ ResourceGroupStorage = (*StorageEndpoint)(nil)

// SaveResourceGroupSetting stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupSetting(name string, msg proto.Message) error {
	return se.saveProto(resourceGroupSettingKeyPath(name), msg)
}

// DeleteResourceGroupSetting removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupSetting(name string) error {
	return se.Remove(resourceGroupSettingKeyPath(name))
}

// LoadResourceGroupSettings loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupSettings(f func(k, v string)) error {
	return se.loadRangeByPrefix(resourceGroupSettingsPath+"/", f)
}

// SaveResourceGroupStates stores a resource group to storage.
func (se *StorageEndpoint) SaveResourceGroupStates(name string, obj interface{}) error {
	return se.saveJSON(resourceGroupStateKeyPath(name), obj)
}

// DeleteResourceGroupStates removes a resource group from storage.
func (se *StorageEndpoint) DeleteResourceGroupStates(name string) error {
	return se.Remove(resourceGroupStateKeyPath(name))
}

// LoadResourceGroupStates loads all resource groups from storage.
func (se *StorageEndpoint) LoadResourceGroupStates(f func(k, v string)) error {
	return se.loadRangeByPrefix(resourceGroupStatesPath+"/", f)
}

// SaveControllerConfig stores the resource controller config to storage.
func (se *StorageEndpoint) SaveControllerConfig(config interface{}) error {
	return se.saveJSON(controllerConfigPath, config)
}
