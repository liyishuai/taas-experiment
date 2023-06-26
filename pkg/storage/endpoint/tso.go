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

package endpoint

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TSOStorage is the interface for timestamp storage.
type TSOStorage interface {
	LoadTimestamp(prefix string) (time.Time, error)
	SaveTimestamp(key string, ts time.Time) error
	LoadTaasTimestamp(prefix string) (int64, error)
	BumpTaasTimestamp(key string, ts int64) error
}

// for Taas timestamp interface check
var _ TSOStorage = (*StorageEndpoint)(nil)

// LoadTimestamp will get all time windows of Local/Global TSOs from etcd and return the biggest one.
// For the Global TSO, loadTimestamp will get all Local and Global TSO time windows persisted in etcd and choose the biggest one.
// For the Local TSO, loadTimestamp will only get its own dc-location time window persisted before.
func (se *StorageEndpoint) LoadTimestamp(prefix string) (time.Time, error) {
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return typeutil.ZeroTime, err
	}
	if len(keys) == 0 {
		return typeutil.ZeroTime, nil
	}

	maxTSWindow := typeutil.ZeroTime
	for i, key := range keys {
		key := strings.TrimSpace(key)
		if !strings.HasSuffix(key, timestampKey) {
			continue
		}
		tsWindow, err := typeutil.ParseTimestamp([]byte(values[i]))
		if err != nil {
			log.Error("parse timestamp window that from etcd failed", zap.String("ts-window-key", key), zap.Time("max-ts-window", maxTSWindow), zap.Error(err))
			continue
		}
		if typeutil.SubRealTimeByWallClock(tsWindow, maxTSWindow) > 0 {
			maxTSWindow = tsWindow
		}
	}
	return maxTSWindow, nil
}

// SaveTimestamp saves the timestamp to the storage.
func (se *StorageEndpoint) SaveTimestamp(key string, ts time.Time) error {
	return se.RunInTxn(context.Background(), func(txn kv.Txn) error {
		value, err := txn.Load(key)
		if err != nil {
			return err
		}

		previousTS := typeutil.ZeroTime
		if value != "" {
			previousTS, err = typeutil.ParseTimestamp([]byte(value))
			if err != nil {
				log.Error("parse timestamp failed", zap.String("key", key), zap.String("value", value), zap.Error(err))
				return err
			}
		}
		if previousTS != typeutil.ZeroTime && typeutil.SubRealTimeByWallClock(ts, previousTS) <= 0 {
			return nil
		}
		data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
		return txn.Save(key, string(data))
	})
}

func (se *StorageEndpoint) LoadTaasTimestamp(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		// file not exist, start from 0
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	dataStr, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Read from file error:", err)
		return -1, err
	}

	var maxTs uint64
	maxTs, err = strconv.ParseUint(strings.TrimSpace(dataStr), 10, 64)
	if err != nil {
		fmt.Println("Convert data error:", err)
		return -1, err
	}
	return int64(maxTs), nil
}

// TickTimestamp saves the timestamp to the storage.
func (se *StorageEndpoint) BumpTaasTimestamp(filePath string, ts int64) error {
	currentTs := uint64(ts)
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Println("Create file error:", err)
		return	err
	}
	defer file.Close()	

	writer := bufio.NewWriter(file)

	_, err = fmt.Fprintf(writer, "%d", currentTs)
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}
