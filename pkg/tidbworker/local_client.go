// Copyright 2024 PingCAP, Inc.
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

package tidbworker

import (
	"context"

	workercliV2 "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/clientv2"
)

type localClient struct {
}

var _ workercliV2.Client = localClient{}

func (l localClient) Ping(ctx context.Context) error {
	return nil
}

func (l localClient) RegisterGC(ctx context.Context, deletionTs uint64) error {
	return nil
}

func (l localClient) RecycleGC(ctx context.Context, safePoint uint64) error {
	return nil
}

func (l localClient) RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error {
	return nil
}

func (l localClient) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	return nil
}

func (l localClient) UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error {
	return nil
}

func (l localClient) GetBgTaskConfig(ctx context.Context, workerType string) (workerCount int, autoScaleEnabled bool, err error) {
	return 0, false, nil
}

func (l localClient) RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error {
	return nil
}

func (l localClient) RecycleBgTask(ctx context.Context, gTaskID, subTaskID int64) error {
	return nil
}

func (l localClient) UpdateBgTaskExecID(ctx context.Context, gTaskID int64, subTaskIDs []int64, execIDs []string) error {
	return nil
}

func (l localClient) RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error {
	return nil
}

func (l localClient) RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error {
	return nil
}

func (l localClient) DeleteTTLTableInfo(ctx context.Context, tableID int64) error {
	return nil
}

func (l localClient) RecycleTTLTask(ctx context.Context, finishTime uint64) error {
	return nil
}

func (l localClient) UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error {
	return nil
}

func (l localClient) RegisterAutoAnalyze(ctx context.Context, taskID uint64) error {
	return nil
}

func (l localClient) RecycleAutoAnalyze(ctx context.Context, taskID uint64) error {
	return nil
}
