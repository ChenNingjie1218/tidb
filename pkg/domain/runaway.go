// Copyright 2023 PingCAP, Inc.
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

package domain

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

func (do *Domain) initResourceGroupsController(ctx context.Context, pdClient pd.Client, uniqueID uint64) error {
	if pdClient == nil {
		logutil.BgLogger().Warn("cannot setup up resource controller, not using tikv storage")
		// return nil as unistore doesn't support it
		return nil
	}

	ruConfig := config.GetGlobalConfig().RUConfig
	log.Info("request unit config", zap.Any("ruConfig", ruConfig))

	var opts []rmclient.ResourceControlCreateOption
	if config.GetGlobalConfig().EnableRULimit {
		opts = []rmclient.ResourceControlCreateOption{
			rmclient.EnableSingleGroupByKeyspace(),
			rmclient.WithMaxWaitDuration(time.Hour),
			rmclient.WithDegradedModeWaitDuration(time.Second * 3 / 2), // 1.5s
		}

		if strings.Contains(os.Getenv("NAMESPACE"), "vip") { // enlarge wait retry for vip clusters, 2s total wait time.
			opts = append(opts,
				rmclient.WithWaitRetryInterval(100*time.Millisecond),
				rmclient.WithWaitRetryTimes(20),
			)
		}
	}

	control, err := rmclient.NewResourceGroupController(ctx, uniqueID, pdClient, &ruConfig, opts...)
	if err != nil {
		return err
	}
	control.Start(ctx)
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	serverAddr := net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	do.runawayManager = runaway.NewRunawayManager(control, serverAddr,
		do.sysSessionPool, do.exit, do.infoCache, do.ddl)
	do.resourceGroupsController = control
	tikv.SetResourceControlInterceptor(control)
	return nil
}
