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

package stmtsummary

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var stmtLogEncoderPool = buffer.NewPool()

type stmtLogStorage struct {
	logger         *zap.Logger
	unredactLogger *zap.Logger
}

func newStmtLogStorage(cfg *log.Config) *stmtLogStorage {
	// Create the stmt logger
	logger, prop, err := log.InitLogger(cfg)
	if err != nil {
		logutil.BgLogger().Error("failed to init logger", zap.Error(err))
		return &stmtLogStorage{logger: zap.NewNop()}
	}
	// Replace 2018-12-19-unified-log-format text encoder with statements encoder
	newCore := log.NewTextCore(&stmtLogEncoder{}, prop.Syncer, prop.Level)
	logger = logger.WithOptions(zap.WrapCore(func(zapcore.Core) zapcore.Core {
		return newCore
	}))

	globalCfg := config.GetGlobalConfig()
	if globalCfg.EnableUnredactLogger && globalCfg.KeyspaceName != "" {
		if err := logutil.RemoveOtherKeyspaceFiles(globalCfg.KeyspaceName, cfg.File.Filename); err != nil {
			logutil.BgLogger().Info("remove other keyspace files failed", zap.Error(err))
		}
		unredactCfg := *cfg
		unredactCfg.File.Filename = logutil.AddPrefixToFilepath(globalCfg.KeyspaceName, cfg.File.Filename)
		unredactLogger, unredactProp, err := log.InitLogger(&unredactCfg)
		if err != nil {
			logutil.BgLogger().Error("failed to init unredact logger", zap.Error(err))
			return &stmtLogStorage{logger: logger}
		}
		newUnredactCore := log.NewTextCore(&stmtLogEncoder{}, unredactProp.Syncer, unredactProp.Level)
		unredactLogger = unredactLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return newUnredactCore
		}))
		return &stmtLogStorage{logger: logger, unredactLogger: unredactLogger}
	}

	return &stmtLogStorage{logger: logger}
}

func (s *stmtLogStorage) persist(w *stmtWindow, end time.Time) {
	begin := w.begin.Unix()
	for _, v := range w.lru.Values() {
		r := v.(*lockedStmtRecord)
		r.Lock()
		r.Begin = begin
		r.End = end.Unix()
		s.log(s.logger, r.StmtRecord)
		if s.unredactLogger != nil && !r.IsInternal {
			unredactRecord := *r.StmtRecord
			unredactRecord.SampleSQL = unredactRecord.UnredactSQL
			s.log(s.unredactLogger, &unredactRecord)
		}
		r.Unlock()
	}
	w.evicted.Lock()
	if w.evicted.other.ExecCount > 0 {
		w.evicted.other.Begin = begin
		w.evicted.other.End = end.Unix()
		s.log(s.logger, w.evicted.other)
	}
	w.evicted.Unlock()
}

func (s *stmtLogStorage) sync() error {
	return s.logger.Sync()
}

func (s *stmtLogStorage) log(logger *zap.Logger, r *StmtRecord) {
	b, err := json.Marshal(r)
	if err != nil {
		logutil.BgLogger().Warn("failed to marshal statement summary", zap.Error(err))
		return
	}
	logger.Info(string(b))
}

type stmtLogEncoder struct{}

func (*stmtLogEncoder) EncodeEntry(entry zapcore.Entry, _ []zapcore.Field) (*buffer.Buffer, error) {
	b := stmtLogEncoderPool.Get()
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b, nil
}

func (e *stmtLogEncoder) Clone() zapcore.Encoder                        { return e }
func (*stmtLogEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return nil }
func (*stmtLogEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return nil }
func (*stmtLogEncoder) AddBinary(string, []byte)                        {}
func (*stmtLogEncoder) AddByteString(string, []byte)                    {}
func (*stmtLogEncoder) AddBool(string, bool)                            {}
func (*stmtLogEncoder) AddComplex128(string, complex128)                {}
func (*stmtLogEncoder) AddComplex64(string, complex64)                  {}
func (*stmtLogEncoder) AddDuration(string, time.Duration)               {}
func (*stmtLogEncoder) AddFloat64(string, float64)                      {}
func (*stmtLogEncoder) AddFloat32(string, float32)                      {}
func (*stmtLogEncoder) AddInt(string, int)                              {}
func (*stmtLogEncoder) AddInt64(string, int64)                          {}
func (*stmtLogEncoder) AddInt32(string, int32)                          {}
func (*stmtLogEncoder) AddInt16(string, int16)                          {}
func (*stmtLogEncoder) AddInt8(string, int8)                            {}
func (*stmtLogEncoder) AddString(string, string)                        {}
func (*stmtLogEncoder) AddTime(string, time.Time)                       {}
func (*stmtLogEncoder) AddUint(string, uint)                            {}
func (*stmtLogEncoder) AddUint64(string, uint64)                        {}
func (*stmtLogEncoder) AddUint32(string, uint32)                        {}
func (*stmtLogEncoder) AddUint16(string, uint16)                        {}
func (*stmtLogEncoder) AddUint8(string, uint8)                          {}
func (*stmtLogEncoder) AddUintptr(string, uintptr)                      {}
func (*stmtLogEncoder) AddReflected(string, any) error                  { return nil }
func (*stmtLogEncoder) OpenNamespace(string)                            {}
