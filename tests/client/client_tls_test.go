// Copyright 2020 TiKV Project Authors.
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

package client_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/netutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.etcd.io/etcd/pkg/transport"
	"google.golang.org/grpc"
)

var (
	testTLSInfo = transport.TLSInfo{
		KeyFile:       "./cert/pd-server-key.pem",
		CertFile:      "./cert/pd-server.pem",
		TrustedCAFile: "./cert/ca.pem",
	}

	testClientTLSInfo = transport.TLSInfo{
		KeyFile:       "./cert/client-key.pem",
		CertFile:      "./cert/client.pem",
		TrustedCAFile: "./cert/ca.pem",
	}

	testTLSInfoExpired = transport.TLSInfo{
		KeyFile:       "./cert-expired/pd-server-key.pem",
		CertFile:      "./cert-expired/pd-server.pem",
		TrustedCAFile: "./cert-expired/ca.pem",
	}
)

// TestTLSReloadAtomicReplace ensures server reloads expired/valid certs
// when all certs are atomically replaced by directory renaming.
// And expects server to reject client requests, and vice versa.
func TestTLSReloadAtomicReplace(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tmpDir := t.TempDir()
	os.RemoveAll(tmpDir)

	certsDir := t.TempDir()

	certsDirExp := t.TempDir()

	cloneFunc := func() transport.TLSInfo {
		tlsInfo, terr := copyTLSFiles(testTLSInfo, certsDir)
		re.NoError(terr)
		_, err := copyTLSFiles(testTLSInfoExpired, certsDirExp)
		re.NoError(err)
		return tlsInfo
	}
	replaceFunc := func() {
		err := os.Rename(certsDir, tmpDir)
		re.NoError(err)
		err = os.Rename(certsDirExp, certsDir)
		re.NoError(err)
		// after rename,
		// 'certsDir' contains expired certs
		// 'tmpDir' contains valid certs
		// 'certsDirExp' does not exist
	}
	revertFunc := func() {
		err := os.Rename(tmpDir, certsDirExp)
		re.NoError(err)

		err = os.Rename(certsDir, tmpDir)
		re.NoError(err)

		err = os.Rename(certsDirExp, certsDir)
		re.NoError(err)
	}
	testTLSReload(re, ctx, cloneFunc, replaceFunc, revertFunc)
}

func testTLSReload(
	re *require.Assertions,
	ctx context.Context,
	cloneFunc func() transport.TLSInfo,
	replaceFunc func(),
	revertFunc func()) {
	tlsInfo := cloneFunc()
	// 1. start cluster with valid certs
	clus, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Security.TLSConfig = grpcutil.TLSConfig{
			KeyPath:  tlsInfo.KeyFile,
			CertPath: tlsInfo.CertFile,
			CAPath:   tlsInfo.TrustedCAFile,
		}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	re.NoError(err)
	defer clus.Destroy()
	err = clus.RunInitialServers()
	re.NoError(err)
	clus.WaitLeader()

	testServers := clus.GetServers()
	endpoints := make([]string, 0, len(testServers))
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
		tlsConfig, err := s.GetConfig().Security.ToTLSConfig()
		re.NoError(err)
		httpClient := &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig:   tlsConfig,
			},
		}
		re.True(netutil.IsEnableHTTPS(httpClient))
	}
	// 2. concurrent client dialing while certs become expired
	errc := make(chan error, 1)
	go func() {
		for {
			dctx, dcancel := context.WithTimeout(ctx, time.Second)
			cli, err := pd.NewClientWithContext(dctx, endpoints, pd.SecurityOption{
				CAPath:   testClientTLSInfo.TrustedCAFile,
				CertPath: testClientTLSInfo.CertFile,
				KeyPath:  testClientTLSInfo.KeyFile,
			}, pd.WithGRPCDialOptions(grpc.WithBlock()))
			if err != nil {
				errc <- err
				dcancel()
				return
			}
			dcancel()
			cli.Close()
		}
	}()

	// 3. replace certs with expired ones
	replaceFunc()

	// 4. expect dial time-out when loading expired certs
	select {
	case cerr := <-errc:
		re.Contains(cerr.Error(), "failed to get cluster id")
	case <-time.After(5 * time.Second):
		re.FailNow("failed to receive dial timeout error")
	}

	// 5. replace expired certs back with valid ones
	revertFunc()

	// 6. new requests should trigger listener to reload valid certs
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	cli, err := pd.NewClientWithContext(dctx, endpoints, pd.SecurityOption{
		CAPath:   testClientTLSInfo.TrustedCAFile,
		CertPath: testClientTLSInfo.CertFile,
		KeyPath:  testClientTLSInfo.KeyFile,
	}, pd.WithGRPCDialOptions(grpc.WithBlock()))
	re.NoError(err)
	dcancel()
	cli.Close()

	// 7. test use raw bytes to init tls config
	caData, certData, keyData := loadTLSContent(re,
		testClientTLSInfo.TrustedCAFile, testClientTLSInfo.CertFile, testClientTLSInfo.KeyFile)
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	_, err = pd.NewClientWithContext(ctx1, endpoints, pd.SecurityOption{
		SSLCABytes:   caData,
		SSLCertBytes: certData,
		SSLKEYBytes:  keyData,
	}, pd.WithGRPCDialOptions(grpc.WithBlock()))
	re.NoError(err)
	cancel1()
}

func loadTLSContent(re *require.Assertions, caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	var err error
	caData, err = os.ReadFile(caPath)
	re.NoError(err)
	certData, err = os.ReadFile(certPath)
	re.NoError(err)
	keyData, err = os.ReadFile(keyPath)
	re.NoError(err)
	return
}

// copyTLSFiles clones certs files to dst directory.
func copyTLSFiles(ti transport.TLSInfo, dst string) (transport.TLSInfo, error) {
	ci := transport.TLSInfo{
		KeyFile:        filepath.Join(dst, "pd-server-key.pem"),
		CertFile:       filepath.Join(dst, "pd-server.pem"),
		TrustedCAFile:  filepath.Join(dst, "ca.pem"),
		ClientCertAuth: ti.ClientCertAuth,
	}
	if err := copyFile(ti.KeyFile, ci.KeyFile); err != nil {
		return transport.TLSInfo{}, err
	}
	if err := copyFile(ti.CertFile, ci.CertFile); err != nil {
		return transport.TLSInfo{}, err
	}
	if err := copyFile(ti.TrustedCAFile, ci.TrustedCAFile); err != nil {
		return transport.TLSInfo{}, err
	}
	return ci, nil
}

func copyFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("Error closing file: %s\n", err)
		}
	}()

	w, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer w.Close()

	if _, err = io.Copy(w, f); err != nil {
		return err
	}
	return w.Sync()
}
