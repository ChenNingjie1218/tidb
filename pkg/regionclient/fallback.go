// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package regionclient

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/sony/gobreaker"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	_ pd.Client = &ClientWithFallback{}
)

// ClientWithFallback is a pd client with fallback support,
// while the default PD client is unavailable, it will fallback to a CSE PD client.
type ClientWithFallback struct {
	pd.Client
	cse     pd.Client
	breaker *asyncBreaker
}

// CBOptions is a wrapper for gobreaker settings.
type CBOptions struct {
	Interval      time.Duration
	Timeout       time.Duration
	ProbeInterval time.Duration
	ReadyToTrip   func(counts gobreaker.Counts) bool
}

func defaultCBOptions() *CBOptions {
	return &CBOptions{
		Interval:      5 * time.Second,
		Timeout:       1 * time.Second,
		ProbeInterval: 1 * time.Second,
		ReadyToTrip:   ifMostFailures,
	}
}

func ifMostFailures(counts gobreaker.Counts) bool {
	failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
	return counts.Requests >= 5 && failureRatio >= 0.4
}

func probePD(name string, client pd.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.GetRegionByID(ctx, 1)
	if err == nil {
		log.Warn("mark pd client as available", zap.String("name", name))
		return nil
	}
	log.Warn("pd client still unavailable", zap.String("name", name))
	return err
}

// NewClientWithFallback creates a ClientWithFallback instance.
func NewClientWithFallback(client pd.Client, tlsConfig *tls.Config, cbOpt *CBOptions) (pd.Client, error) {
	if cbOpt == nil {
		cbOpt = defaultCBOptions()
	}

	f := &ClientWithFallback{
		Client: client,
	}
	cse, err := NewClient(client, tlsConfig, cbOpt)
	if err != nil {
		return nil, err
	}
	f.cse = cse

	s := settings{
		Name:          "pd-fallback-client",
		Interval:      cbOpt.Interval,
		Timeout:       cbOpt.Timeout,
		ProbeInterval: cbOpt.ProbeInterval,
		ReadyToTrip:   cbOpt.ReadyToTrip,
		Probe: func(name string) error {
			log.Warn("origin pd client unavailable, start probing", zap.String("name", name))
			return probePD(name, client, 1*time.Second)
		},
	}

	breaker := newAsyncBreaker(s)
	f.breaker = breaker

	return f, nil
}

// GetRegion implements pd.Client interface
func (f *ClientWithFallback) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (any, error) {
		return f.Client.GetRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegion(ctx, key, opts...)
}

// GetPrevRegion implements pd.Client interface
func (f *ClientWithFallback) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (any, error) {
		return f.Client.GetPrevRegion(ctx, key, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetPrevRegion(ctx, key, opts...)
}

// GetRegionByID implements pd.Client interface
func (f *ClientWithFallback) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (any, error) {
		return f.Client.GetRegionByID(ctx, regionID, opts...)
	})
	if err == nil {
		return resp.(*pd.Region), nil
	}
	return f.cse.GetRegionByID(ctx, regionID, opts...)
}

// ScanRegions implements pd.Client interface
func (f *ClientWithFallback) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...pd.GetRegionOption) ([]*pd.Region, error) {
	resp, err := f.breaker.Execute(func() (any, error) {
		return f.Client.ScanRegions(ctx, key, endKey, limit, opts...)
	})
	if err == nil {
		return resp.([]*pd.Region), nil
	}
	return f.cse.ScanRegions(ctx, key, endKey, limit, opts...)
}

// GetStore implements pd.Client interface
func (f *ClientWithFallback) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	resp, err := f.breaker.Execute(func() (any, error) {
		return f.Client.GetStore(ctx, storeID)
	})
	if err == nil {
		return resp.(*metapb.Store), nil
	}
	return f.cse.GetStore(ctx, storeID)
}

// GetAllStores implements pd.Client interface
func (f *ClientWithFallback) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	resp, err := f.breaker.Execute(func() (any, error) {
		return f.Client.GetAllStores(ctx, opts...)
	})
	if err == nil {
		return resp.([]*metapb.Store), nil
	}
	return f.cse.GetAllStores(ctx, opts...)
}

// Close the pd clients and circuit breaker.
func (f *ClientWithFallback) Close() {
	f.cse.Close()
	f.breaker.Close()
}
