// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keyspace

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"

	// KeyspaceMetaConfigGCManagementType is the name of the key for GC management type in keyspace meta config.
	KeyspaceMetaConfigGCManagementType = "safe_point_version"
	// KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC is a type of GC management in keyspace meta config,
	// it means this keyspace will calculate GC safe point by its own.
	KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC = "v2"
	// KeyspaceMetaConfigGCManagementTypeGlobalGC is a type of GC management in keyspace meta config, it means this keyspace will use GC safe point by global GC(default).
	KeyspaceMetaConfigGCManagementTypeGlobalGC = "global_gc"

	// maxKeyspaceID is the maximum keyspace id that can be created, no keyspace can be created greater than this value.
	maxKeyspaceID = 0xffffff

	// API V2 key bounds:
	// See https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md for more keyspace and API V2 details.

	// KeyspaceTxnModePrefix is txn data prefix of keyspace.
	KeyspaceTxnModePrefix byte = 'x'
	// MaxKeyspaceRightBoundaryPrefix is the max keyspace txn right boundary.
	// Because the keyspace ranges are all under the 'x' prefix, so MaxKeyspaceRightBoundaryPrefix = 'y'
	MaxKeyspaceRightBoundaryPrefix byte = 'y'
	// RawKVLeftBoundaryPrefix is RawKV left boundary.
	RawKVLeftBoundaryPrefix byte = 'r'
	// RawKVRightBoundaryPrefix is RawKV right boundary.
	RawKVRightBoundaryPrefix byte = 's'

	// KeyspaceMetaConfigIsInETCDMigration is the name of the key for keyspace meta config migration.
	KeyspaceMetaConfigIsInETCDMigration = "serverless_is_upgrade_etcd_group"
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// MakeKeyspaceEtcdNamespace return the keyspace prefix path for etcd namespace
func MakeKeyspaceEtcdNamespace(c tikv.Codec) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", c.GetKeyspaceID())
}

// MakeKeyspaceEtcdNamespaceSlash return the keyspace prefix path for etcd namespace, and end with a slash.
func MakeKeyspaceEtcdNamespaceSlash(c tikv.Codec) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d/", c.GetKeyspaceID())
}

// GetKeyspaceNameBySettings is used to get Keyspace name setting.
func GetKeyspaceNameBySettings() (keyspaceName string) {
	keyspaceName = config.GetGlobalKeyspaceName()
	return keyspaceName
}

// IsKeyspaceNameEmpty is used to determine whether keyspaceName is set.
func IsKeyspaceNameEmpty(keyspaceName string) bool {
	return keyspaceName == ""
}

// WrapZapcoreWithKeyspace is used to wrap zapcore.Core.
func WrapZapcoreWithKeyspace() zap.Option {
	return zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		keyspaceName := GetKeyspaceNameBySettings()
		if !IsKeyspaceNameEmpty(keyspaceName) {
			core = core.With([]zap.Field{zap.String("keyspaceName", keyspaceName)})
		}
		return core
	})
}

// BuildAPIContext is used to build APIContext.
func BuildAPIContext(keyspaceName string) (apiContext pd.APIContext) {
	if len(keyspaceName) == 0 {
		apiContext = pd.NewAPIContextV1()
	} else {
		apiContext = pd.NewAPIContextV2(keyspaceName)
	}
	return
}

// CodecFromName is used to get the corresponding codec according to the keyspace name.
func CodecFromName(ctx context.Context, pdCli pd.Client, keyspaceName string) (tikv.Codec, error) {
	if len(keyspaceName) == 0 {
		return tikv.NewCodecV1(tikv.ModeTxn), nil
	}

	meta, err := pdCli.LoadKeyspace(ctx, keyspaceName)
	if err != nil {
		return nil, err
	}

	return tikv.NewCodecV2(tikv.ModeTxn, meta)
}

// IsKeyspaceUseKeyspaceLevelGC returns true if keyspace meta config set "gc_management_type" = "keyspace_level_gc" explicitly.
func IsKeyspaceUseKeyspaceLevelGC(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	if keyspaceMeta == nil {
		return false
	}
	if val, ok := keyspaceMeta.Config[KeyspaceMetaConfigGCManagementType]; ok {
		return val == KeyspaceMetaConfigGCManagementTypeKeyspaceLevelGC
	}
	return false
}

// IsKeyspaceMetaNotNilAndUseGlobalGC returns whether the specified keyspace meta use global GC.
func IsKeyspaceMetaNotNilAndUseGlobalGC(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	if keyspaceMeta == nil {
		return false
	}
	if val, ok := keyspaceMeta.Config[KeyspaceMetaConfigGCManagementType]; ok {
		return val == KeyspaceMetaConfigGCManagementTypeGlobalGC
	}
	return true
}

// GetKeyspaceTxnLeftBound returns the specified keyspace txn left boundary.
func GetKeyspaceTxnLeftBound(keyspaceID uint32) []byte {
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)
	txnLeftBound := codec.EncodeBytes(nil, append([]byte{'x'}, keyspaceIDBytes[1:]...))
	return txnLeftBound
}

// GetKeyspaceTxnRange returns the specified keyspace txn left boundary and txn right boundary.
func GetKeyspaceTxnRange(keyspaceID uint32) ([]byte, []byte) {
	// Get keyspace txn left boundary
	txnLeftBound := GetKeyspaceTxnLeftBound(keyspaceID)

	var txnRightBound []byte
	if keyspaceID == maxKeyspaceID {
		// Directly set the right boundary of maxKeyspaceID to be {MaxKeyspaceRightBoundaryPrefix}
		maxKeyspaceIDTxnRightBound := []byte{MaxKeyspaceRightBoundaryPrefix}
		txnRightBound = codec.EncodeBytes(nil, maxKeyspaceIDTxnRightBound[:])
	} else {
		// The right boundary of the specified keyspace is the left boundary of keyspaceID + 1.
		txnRightBound = GetKeyspaceTxnLeftBound(keyspaceID + 1)
	}

	return txnLeftBound, txnRightBound
}

// GetPDClient is used to get pd client by etcd addrs and keyspace name.
func GetPDClient(keyspaceName string, etcdAddrs []string) (pd.Client, error) {
	cfg := config.GetGlobalConfig()
	opts := append(cfg.GetPDClientOpts(),
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256*1024*1024)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256*1024*1024)),
		),
		pd.WithForwardingOption(cfg.EnableForwarding),
		// pd.WithMetricsLabels(metrics.GetConstLabels()),
	)
	pdCli, err := pd.NewClientWithAPIContext(context.Background(), BuildAPIContext(keyspaceName),
		etcdAddrs, pd.SecurityOption{
			CAPath:   cfg.Security.ClusterSSLCA,
			CertPath: cfg.Security.ClusterSSLCert,
			KeyPath:  cfg.Security.ClusterSSLKey,
		}, opts...)
	return pdCli, err
}

// EtcdNamespace return the keyspace prefix path for etcd namespace
func EtcdNamespace(c tikv.Codec) string {
	if c == nil || c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", c.GetKeyspaceID())
}

// NewEtcdSafePointKVWithCodec is used to add prefix when set keyspace.
func NewEtcdSafePointKVWithCodec(etcdAddrs []string, codec tikv.Codec, tlsConfig *tls.Config) (*tikv.EtcdSafePointKV, error) {
	var etcdNameSpace string
	if IsKeyspaceUseKeyspaceLevelGC(codec.GetKeyspaceMeta()) {
		etcdNameSpace = EtcdNamespace(codec)
	}
	return tikv.NewEtcdSafePointKV(etcdAddrs, tlsConfig, tikv.WithPrefix(etcdNameSpace))
}
