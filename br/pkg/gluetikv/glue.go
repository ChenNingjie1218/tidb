// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetikv

import (
	"context"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver"
	tikvconfig "github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
)

// Asserting Glue implements glue.ConsoleGlue and glue.Glue at compile time.
var (
	_ glue.ConsoleGlue = Glue{}
	_ glue.Glue        = Glue{}
)

// Glue is an implementation of glue.Glue that accesses only TiKV without TiDB.
type Glue struct {
	glue.StdIOGlue
}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	return nil, nil
}

// Open implements glue.Glue.
func (Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	if option.CAPath != "" {
		conf := config.GetGlobalConfig()
		conf.Security.ClusterSSLCA = option.CAPath
		conf.Security.ClusterSSLCert = option.CertPath
		conf.Security.ClusterSSLKey = option.KeyPath
		config.StoreGlobalConfig(conf)
	}

	keyspaceMeta, err := getKeyspaceMeta(path)
	if err != nil {
		return nil, err
	}
	opt := &kv.DriverOpenOption{KeyspaceMeta: keyspaceMeta}
	return driver.TiKVDriver{}.Open(path, opt)
}

func getKeyspaceMeta(path string) (*keyspacepb.KeyspaceMeta, error) {
	cfg := config.GetGlobalConfig()
	etcdAddrs, _, _, err := tikvconfig.ParsePath(path)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	clientOpts := append(cfg.GetPDClientOpts(), pd.WithInitMetricsOption(false))
	pdCli, err := pd.NewClientWithAPIContext(ctx, keyspace.BuildAPIContext(cfg.KeyspaceName), etcdAddrs, pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}, clientOpts...)
	if err != nil {
		return nil, err
	}
	keyspaceMeta, err := pdCli.LoadKeyspace(ctx, cfg.KeyspaceName)
	if err != nil {
		return nil, err
	}
	return keyspaceMeta, nil
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return utils.StartProgress(ctx, cmdName, total, redirectLog, nil)
}

// Record implements glue.Glue.
func (Glue) Record(name string, val uint64) {
	summary.CollectSuccessUnit(name, 1, val)
}

// GetVersion implements glue.Glue.
func (Glue) GetVersion() string {
	return "BR\n" + build.Info()
}

// UseOneShotSession implements glue.Glue.
func (g Glue) UseOneShotSession(store kv.Storage, closeDomain bool, fn func(glue.Session) error) error {
	return nil
}

func (Glue) GetClient() glue.GlueClient {
	return glue.ClientCLP
}
