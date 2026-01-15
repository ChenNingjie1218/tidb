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

package importinto

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/executor/importer"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type importIntoSuite struct {
	suite.Suite
}

func TestImportInto(t *testing.T) {
	suite.Run(t, &importIntoSuite{})
}

func (s *importIntoSuite) enableFailPoint(path, term string) {
	require.NoError(s.T(), failpoint.Enable(path, term))
	s.T().Cleanup(func() {
		_ = failpoint.Disable(path)
	})
}

func (s *importIntoSuite) TestSchedulerGetEligibleInstances() {
	sch := ImportSchedulerExt{}
	ctx := context.Background()

	// Test empty EligibleInstances
	task := &proto.Task{Meta: []byte("{}")}
	eligibleInstances, err := sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	s.Empty(eligibleInstances)

	// Test EligibleInstances with first instance in live executors
	s.enableFailPoint("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTaskExecutorNodes", "return()")
	liveExecIDs := []string{"1.1.1.1:4000", "2.2.2.2:4001"}
	scheduler.MockServerInfo.Store(&liveExecIDs)
	task.Meta = []byte(`{"EligibleInstances":[{"ip": "1.1.1.1", "listening_port": 4000}]}`)
	eligibleInstances, err = sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	s.Equal([]string{"1.1.1.1:4000"}, eligibleInstances)

	// Test EligibleInstances with first instance not in live executors (fallback to current server)
	// Note: This will fallback to current server's execID, which requires GetServerInfo to work
	// Mock GetServerInfo to return a valid server info
	mockServerInfoJSON := `{"ip": "127.0.0.1", "listening_port": 4000, "status_port": 10080, "ddl_id": "test-id", "server_id": 1, "start_timestamp": 1234567890, "lease": "45s", "labels": {}}`
	s.enableFailPoint("github.com/pingcap/tidb/pkg/domain/infosync/mockGetServerInfo", fmt.Sprintf("return(`%s`)", mockServerInfoJSON))
	task.Meta = []byte(`{"EligibleInstances":[{"ip": "3.3.3.3", "listening_port": 4000}]}`)
	eligibleInstances, err = sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	// When the eligible instance is not in live executors, it falls back to current server
	// The result should not be empty (contains current server's execID)
	s.NotEmpty(eligibleInstances)
	s.Len(eligibleInstances, 1)
	s.Equal([]string{"127.0.0.1:4000"}, eligibleInstances)
}

func (s *importIntoSuite) TestUpdateCurrentTask() {
	taskMeta := TaskMeta{
		Plan: importer.Plan{
			DisableTiKVImportMode: true,
		},
	}
	bs, err := json.Marshal(taskMeta)
	require.NoError(s.T(), err)

	sch := ImportSchedulerExt{}
	require.Equal(s.T(), int64(0), sch.currTaskID.Load())
	require.False(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		TaskBase: proto.TaskBase{ID: 1},
		Meta:     bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		TaskBase: proto.TaskBase{ID: 1},
		Meta:     bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())
}

func (s *importIntoSuite) TestSchedulerInit() {
	meta := TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "",
		},
	}
	bytes, err := json.Marshal(meta)
	s.NoError(err)
	sch := importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			Meta: bytes,
		}, scheduler.Param{}),
	}
	s.NoError(sch.Init())
	s.False(sch.Extension.(*ImportSchedulerExt).GlobalSort)

	meta.Plan.CloudStorageURI = "s3://test"
	bytes, err = json.Marshal(meta)
	s.NoError(err)
	sch = importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			Meta: bytes,
		}, scheduler.Param{}),
	}
	s.NoError(sch.Init())
	s.True(sch.Extension.(*ImportSchedulerExt).GlobalSort)
}

func (s *importIntoSuite) TestGetNextStep() {
	task := &proto.TaskBase{Step: proto.StepInit}
	ext := &ImportSchedulerExt{}
	for _, nextStep := range []proto.Step{proto.ImportStepImport, proto.ImportStepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}

	task.Step = proto.StepInit
	ext = &ImportSchedulerExt{GlobalSort: true}
	for _, nextStep := range []proto.Step{proto.ImportStepEncodeAndSort, proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest, proto.ImportStepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}
}

func (s *importIntoSuite) TestGetStepOfEncode() {
	s.Equal(proto.ImportStepImport, getStepOfEncode(false, false))
	s.Equal(proto.ImportStepEncodeAndSort, getStepOfEncode(true, false))
}

func (s *importIntoSuite) TestIsRetryable() {
	ext := &ImportSchedulerExt{}
	require.True(s.T(), ext.IsRetryableErr(drivererr.ErrRegionUnavailable))
}

func TestIsImporting2TiKV(t *testing.T) {
	ext := &ImportSchedulerExt{}
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepEncodeAndSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepMergeSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepPostProcess}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepWriteAndIngest}}))
}
