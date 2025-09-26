// Copyright 2018 PingCAP, Inc.
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

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/metaservice"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// Node organizes the ectd query result as a Trie tree
type Node struct {
	Childs map[string]*Node
	Value  []byte
}

// OpType is operation's type in etcd
type OpType string

var (
	// CreateOp is create operation type
	CreateOp OpType = "create"

	// UpdateOp is update operation type
	UpdateOp OpType = "update"

	// DeleteOp is delete operation type
	DeleteOp OpType = "delete"
)

// Operation represents an operation in etcd, include create, update and delete.
type Operation struct {
	Tp         OpType
	Key        string
	Value      string
	Opts       []clientv3.OpOption
	TTL        int64
	WithPrefix bool
}

// String implements Stringer interface.
func (o *Operation) String() string {
	return fmt.Sprintf("{Tp: %s, Key: %s, Value: %s, TTL: %d, WithPrefix: %v, Opts: %v}", o.Tp, o.Key, o.Value, o.TTL, o.WithPrefix, o.Opts)
}

// Client is a wrapped etcd client that support some simple method
type Client struct {
	client   *clientv3.Client
	rootPath string
}

// NewClient returns a wrapped etcd client
func NewClient(cli *clientv3.Client, root string) *Client {
	return &Client{
		client:   cli,
		rootPath: root,
	}
}

// Close shutdowns the connection to etcd
func (e *Client) Close() error {
	if err := e.client.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetClient returns client
func (e *Client) GetClient() *clientv3.Client {
	return e.client
}

// Create guarantees to set a key = value with some options(like ttl)
func (e *Client) Create(ctx context.Context, key string, val string, opts []clientv3.OpOption) (int64, error) {
	key = keyWithPrefix(e.rootPath, key)
	txnResp, err := e.client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return 0, errors.AlreadyExistsf("key %s in etcd", key)
	}

	if txnResp.Header != nil {
		return txnResp.Header.Revision, nil
	}

	// impossible to happen
	return 0, errors.New("revision is unknown")
}

// Get returns a key/value matchs the given key
func (e *Client) Get(ctx context.Context, key string) (value []byte, revision int64, err error) {
	key = keyWithPrefix(e.rootPath, key)
	resp, err := e.client.KV.Get(ctx, key)
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, -1, errors.NotFoundf("key %s in etcd", key)
	}

	return resp.Kvs[0].Value, resp.Header.Revision, nil
}

// Update updates a key/value.
// set ttl 0 to disable the Lease ttl feature
func (e *Client) Update(ctx context.Context, key string, val string, ttl int64) error {
	key = keyWithPrefix(e.rootPath, key)

	var opts []clientv3.OpOption
	if ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return errors.Trace(err)
		}

		opts = []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	}

	txnResp, err := e.client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), ">", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return errors.NotFoundf("key %s in etcd", key)
	}

	return nil
}

// UpdateOrCreate updates a key/value, if the key does not exist then create, or update
func (e *Client) UpdateOrCreate(ctx context.Context, key string, val string, ttl int64) error {
	key = keyWithPrefix(e.rootPath, key)

	var opts []clientv3.OpOption
	if ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return errors.Trace(err)
		}

		opts = []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	}

	_, err := e.client.KV.Do(ctx, clientv3.OpPut(key, val, opts...))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// List returns the trie struct that constructed by the key/value with same prefix
func (e *Client) List(ctx context.Context, key string) (node *Node, revision int64, err error) {
	key = keyWithPrefix(e.rootPath, key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	resp, err := e.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	root := new(Node)
	length := len(key)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) <= length {
			continue
		}

		keyTail := key[length:]
		tailNode := parseToDirTree(root, keyTail)
		tailNode.Value = kv.Value
	}

	return root, resp.Header.Revision, nil
}

// Delete deletes the key/values with matching prefix or key
func (e *Client) Delete(ctx context.Context, key string, withPrefix bool) error {
	key = keyWithPrefix(e.rootPath, key)
	var opts []clientv3.OpOption
	if withPrefix {
		opts = []clientv3.OpOption{clientv3.WithPrefix()}
	}

	_, err := e.client.KV.Delete(ctx, key, opts...)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Watch watchs the events of key with prefix.
func (e *Client) Watch(ctx context.Context, prefix string, revision int64) clientv3.WatchChan {
	if revision > 0 {
		return e.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
	}
	return e.client.Watch(ctx, prefix, clientv3.WithPrefix())
}

// DoTxn does some operation in one transaction.
// Note: should only have one opereration for one key, otherwise will get duplicate key error.
func (e *Client) DoTxn(ctx context.Context, operations []*Operation) (int64, error) {
	cmps := make([]clientv3.Cmp, 0, len(operations))
	ops := make([]clientv3.Op, 0, len(operations))

	for _, operation := range operations {
		operation.Key = keyWithPrefix(e.rootPath, operation.Key)

		if operation.TTL > 0 {
			if operation.Tp == DeleteOp {
				return 0, errors.Errorf("unexpected TTL in delete operation")
			}

			lcr, err := e.client.Lease.Grant(ctx, operation.TTL)
			if err != nil {
				return 0, errors.Trace(err)
			}
			operation.Opts = append(operation.Opts, clientv3.WithLease(lcr.ID))
		}

		if operation.WithPrefix {
			operation.Opts = append(operation.Opts, clientv3.WithPrefix())
		}

		switch operation.Tp {
		case CreateOp:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(operation.Key), "=", 0))
			ops = append(ops, clientv3.OpPut(operation.Key, operation.Value, operation.Opts...))
		case UpdateOp:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(operation.Key), ">", 0))
			ops = append(ops, clientv3.OpPut(operation.Key, operation.Value, operation.Opts...))
		case DeleteOp:
			ops = append(ops, clientv3.OpDelete(operation.Key, operation.Opts...))
		default:
			return 0, errors.Errorf("unknown operation type %s", operation.Tp)
		}
	}

	txnResp, err := e.client.KV.Txn(ctx).If(
		cmps...,
	).Then(
		ops...,
	).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return 0, errors.Errorf("do transaction failed, operations: %+v", operations)
	}

	return txnResp.Header.Revision, nil
}

func parseToDirTree(root *Node, p string) *Node {
	pathDirs := strings.Split(p, "/")
	current := root
	var next *Node
	var ok bool

	for _, dir := range pathDirs {
		if current.Childs == nil {
			current.Childs = make(map[string]*Node)
		}

		next, ok = current.Childs[dir]
		if !ok {
			current.Childs[dir] = new(Node)
			next = current.Childs[dir]
		}

		current = next
	}

	return current
}

func keyWithPrefix(prefix, key string) string {
	if strings.HasPrefix(key, prefix) {
		return key
	}

	return path.Join(prefix, key)
}

// SetEtcdCliByNamespace is used to add an etcd namespace prefix before etcd path.
func SetEtcdCliByNamespace(cli *clientv3.Client, namespacePrefix string) {
	cli.KV = namespace.NewKV(cli.KV, namespacePrefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, namespacePrefix)
	cli.Lease = namespace.NewLease(cli.Lease, namespacePrefix)
}

//---------------------- Meta Service ----------------------

// NewEtcdMetaServiceClientWithKVStore returns an EtcdMetaServiceClient constructed from the configuration.
func NewEtcdMetaServiceClientWithKVStore(store kv.Storage) (*metaservice.EtcdMetaServiceClient, error) {
	// todo: introduce shared etcd client
	cfg := config.GetGlobalConfig()

	codec := store.GetCodec()
	ebd, ok := store.(kv.MetaServiceBackend)
	if !ok {
		return nil, errors.New("tikv store not meta service backend")
	}
	metaServiceInfo, err := ebd.MetaServiceInfo()
	if err != nil {
		return nil, err
	}
	return getMetaServiceClientFromInfo(cfg, metaServiceInfo, codec)

}

func getMetaServiceClientFromInfo(cfg *config.Config, metaServiceInfo *metaservice.Info, codec tikv.Codec) (*metaservice.EtcdMetaServiceClient, error) {
	tlsConfig, err := cfg.GetTiKVConfig().Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	return getMetaServiceClientWithTLSConfig(cfg, tlsConfig, metaServiceInfo, codec)
}

func getMetaServiceClientWithTLSConfig(cfg *config.Config, tlsConfig *tls.Config, metaServiceInfo *metaservice.Info, codec tikv.Codec) (*metaservice.EtcdMetaServiceClient, error) {
	etcdLogCfg := zap.NewProductionConfig()

	backoffCfg := backoff.DefaultConfig
	backoffCfg.MaxDelay = 3 * time.Second

	cli, err := NewCodecClient(clientv3.Config{
		LogConfig:        &etcdLogCfg,
		Endpoints:        metaServiceInfo.KeyspaceMetaGroup.KeyspaceMetaServiceAddrs,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoffCfg,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
		},
		TLS: tlsConfig,
	}, codec)
	return &metaservice.EtcdMetaServiceClient{KeyspaceEtcdCli: cli}, err
}

// NewMetaServiceClientFromCfg returns a creates a new client with the given etcd client config and keyspace codec.
func NewMetaServiceClientFromCfg(pdAddrs []string, root string, codec tikv.Codec) (*Client, error) {
	metaServiceInfo, err := metaservice.GetMetaServiceInfo(codec.GetKeyspaceMeta(), pdAddrs, pdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	metServiceClient, err := getMetaServiceClientFromInfo(config.GetGlobalConfig(), metaServiceInfo, codec)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{
		client:   metServiceClient.GetKeyspaceEtcdCli(),
		rootPath: root,
	}, nil
}

// NewMetaServiceClientWithTLSConfig creates a new client with the given etcd client config, keyspace codec, and TLS configuration.
func NewMetaServiceClientWithTLSConfig(pdAddrs []string, codec tikv.Codec, tlsConfig *tls.Config) (*metaservice.EtcdMetaServiceClient, error) {
	metaServiceInfo, err := metaservice.GetMetaServiceInfo(codec.GetKeyspaceMeta(), pdAddrs, pdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return getMetaServiceClientWithTLSConfig(config.GetGlobalConfig(), tlsConfig, metaServiceInfo, codec)
}

// NewCodecClient creates a new client with the given etcd client config and keyspace codec.
func NewCodecClient(cfg clientv3.Config, codec tikv.Codec) (*clientv3.Client, error) {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return namespacedClient(cli, keyspace.EtcdNamespace(codec)), nil
}

func namespacedClient(cli *clientv3.Client, ns string) *clientv3.Client {
	if ns == "" {
		return cli
	}

	cli.KV = namespace.NewKV(cli.KV, ns)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, ns)
	cli.Lease = namespace.NewLease(cli.Lease, ns)

	return cli
}

// NewEtcdCliNonNamespace exports for init unprefixedEtcdCli and testing.
func NewEtcdCliNonNamespace(addrs []string, ebd kv.MetaServiceBackend, codec tikv.Codec) (*clientv3.Client, error) {
	cfg := config.GetGlobalConfig()
	etcdLogCfg := zap.NewProductionConfig()
	etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	backoffCfg := backoff.DefaultConfig
	backoffCfg.MaxDelay = 3 * time.Second
	cli, err := NewCodecClient(clientv3.Config{
		LogConfig:        &etcdLogCfg,
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoffCfg,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
		},
		TLS: ebd.TLSConfig(),
	}, codec)
	return cli, err
}

// GetPDClient is used to get pd client by etcd addrs and keyspace name.
func GetPDClient(keyspaceName string, pdEtcdAddrs []string) (pd.Client, error) {
	cfg := config.GetGlobalConfig()
	pdCli, err := pd.NewClientWithAPIContext(context.Background(), keyspace.BuildAPIContext(keyspaceName), pdEtcdAddrs,
		pd.SecurityOption{
			CAPath:   cfg.Security.ClusterSSLCA,
			CertPath: cfg.Security.ClusterSSLCert,
			KeyPath:  cfg.Security.ClusterSSLKey,
		},
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256*1024*1024)),
		),
		pd.WithCustomTimeoutOption(time.Duration(cfg.PDClient.PDServerTimeout)*time.Second),
		pd.WithForwardingOption(cfg.EnableForwarding),
	)
	// TODO(metrics)
	// pd.WithMetricsLabels(metrics.GetConstLabels()))
	return pdCli, err
}

func getCodecPDClient(keyspaceName string, pdCli pd.Client) (*tikv.CodecPDClient, error) {
	var err error
	var pdCodecClient *tikv.CodecPDClient
	if keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		pdCodecClient = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
	} else {
		pdCodecClient, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, pdCli, keyspaceName)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return pdCodecClient, err
}

// GetEtcdEndpointsWithPDAddrs gets etcd endpoints with pd etcd addrs and keyspace name.
func GetEtcdEndpointsWithPDAddrs(
	tlsConfig *tls.Config,
	pdEtcdAddrs []string,
	keyspaceName string,
) (*clientv3.Client, error) {
	pdClient, err := GetPDClient(keyspaceName, pdEtcdAddrs)
	if err != nil {
		return nil, err
	}

	pdCodecClient, err := getCodecPDClient(keyspaceName, pdClient)
	if err != nil {
		return nil, errors.Trace(err)
	}

	codec := pdCodecClient.GetCodec()

	metaServiceInfo, err := metaservice.GetMetaServiceInfo(codec.GetKeyspaceMeta(), pdEtcdAddrs, pdEtcdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	metServiceClient, err := getMetaServiceClientWithTLSConfig(config.GetGlobalConfig(), tlsConfig, metaServiceInfo, codec)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return metServiceClient.GetKeyspaceEtcdCli(), nil

}

// GetEtcdClientForTest gets an etcd client for test.
func GetEtcdClientForTest() (*clientv3.Client, error) {
	tidbCfg := config.GetGlobalConfig()
	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	tls, err2 := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
	if err2 != nil {
		return nil, err2
	}

	addrs := strings.Split(tidbCfg.Path, ",")
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		TLS:              tls.TLSConfig(),
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return etcdCli, nil
}
