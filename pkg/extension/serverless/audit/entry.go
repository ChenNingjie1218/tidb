// Copyright 2022 PingCAP, Inc.
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

package audit

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/encrypt"
	"go.uber.org/zap"
)

// EventClass is the class for event
type EventClass int

// The below defines all event classes
const (
	ClassUnknown EventClass = iota
	ClassConnection
	ClassConnect
	ClassDisconnect
	ClassChangeUser
	ClassQuery
	ClassTransaction
	ClassExecute
	ClassDML
	ClassInsert
	ClassReplace
	ClassUpdate
	ClassDelete
	ClassLoadData
	ClassSelect
	ClassDDL
	ClassAudit
	ClassAuditSetSysVar
	ClassAuditFuncCall
	ClassAuditEnable
	ClassAuditDisable

	// ClassCount is the count of all classes
	// New class should be added before it
	ClassCount
)

var class2String = map[EventClass]string{
	ClassConnection:     "CONNECTION",
	ClassConnect:        "CONNECT",
	ClassDisconnect:     "DISCONNECT",
	ClassChangeUser:     "CHANGE_USER",
	ClassQuery:          "QUERY",
	ClassTransaction:    "TRANSACTION",
	ClassExecute:        "EXECUTE",
	ClassDML:            "QUERY_DML",
	ClassInsert:         "INSERT",
	ClassReplace:        "REPLACE",
	ClassUpdate:         "UPDATE",
	ClassDelete:         "DELETE",
	ClassLoadData:       "LOAD_DATA",
	ClassSelect:         "SELECT",
	ClassDDL:            "QUERY_DDL",
	ClassAudit:          "AUDIT",
	ClassAuditSetSysVar: "AUDIT_SET_SYS_VAR",
	ClassAuditFuncCall:  "AUDIT_FUNC_CALL",
	ClassAuditEnable:    "AUDIT_ENABLE",
	ClassAuditDisable:   "AUDIT_DISABLE",
}

var string2class map[string]EventClass

func init() {
	string2class = make(map[string]EventClass, ClassCount)
	for class := ClassUnknown + 1; class < ClassCount; class++ {
		if str, ok := class2String[class]; ok {
			string2class[strings.ToUpper(str)] = class
		} else {
			panic(fmt.Sprintf("string for audit class '%d' not set", class))
		}
	}
}

func getEventClass(s string) (EventClass, bool) {
	if c, ok := string2class[strings.ToUpper(s)]; ok {
		return c, true
	}
	return ClassUnknown, false
}

func (EventClass) displayInLog() bool {
	return true
}

func (c EventClass) String() string {
	return class2String[c]
}

func getStmtClasses(stmt ast.StmtNode, execute bool) []EventClass {
	classes := append(make([]EventClass, 0, 4), ClassQuery)
	if execute {
		classes = append(classes, ClassExecute)
	}
	switch stmt.(type) {
	case *ast.SelectStmt:
		classes = append(classes, ClassSelect)
	case ast.DMLNode:
		classes = appendDMLClass(classes, stmt)
	case ast.DDLNode:
		classes = append(classes, ClassDDL)
	case *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.SavepointStmt, *ast.ReleaseSavepointStmt:
		classes = append(classes, ClassTransaction)
	}
	return classes
}

func appendDMLClass(classes []EventClass, stmt ast.StmtNode) []EventClass {
	switch n := stmt.(type) {
	case *ast.InsertStmt:
		if n.IsReplace {
			classes = append(classes, ClassDML, ClassReplace)
		} else {
			classes = append(classes, ClassDML, ClassInsert)
		}
	case *ast.UpdateStmt:
		classes = append(classes, ClassDML, ClassUpdate)
	case *ast.DeleteStmt:
		classes = append(classes, ClassDML, ClassDelete)
	case *ast.LoadDataStmt:
		classes = append(classes, ClassDML, ClassLoadData)
	case *ast.NonTransactionalDMLStmt:
		classes = appendDMLClass(classes, n.DMLStmt)
	}
	return classes
}

// log keys
const (
	LogKeyID           = "ID"
	LogKeyTime         = "TIME"
	LogKeyEvent        = "EVENT"
	LogKeyUser         = "USER"
	LogKeyRoles        = "ROLES"
	LogKeyConnectionID = "CONNECTION_ID"
	LogKeyGwConnID     = "GATEWAY_CONNECTION_ID"
	LogKeyCurrentDB    = "CURRENT_DB"
	LogKeyTables       = "TABLES"
	LogKeyReason       = "REASON"
	LogKeyStatusCode   = "STATUS_CODE"

	LogKeyConnectionTYPE = "CONNECTION_TYPE"
	LogKeyPID            = "PID"
	LogKeyServerVersion  = "SERVER_VERSION"
	LogKeySSLVersion     = "SSL_VERSION"
	LogKeyHostIP         = "HOST_IP"
	LogKeyHostPort       = "HOST_PORT"
	LogKeyClientIP       = "CLIENT_IP"
	LogKeyClientPort     = "CLIENT_PORT"
	LogKeySQLText        = "SQL_TEXT"
	LogKeyExecuteParams  = "EXECUTE_PARAMS"
	LogKeyAffectedRows   = "AFFECTED_ROWS"

	LogKeyAuditOpTarget = "AUDIT_OP_TARGET"
	LogKeyAuditOpArgs   = "AUDIT_OP_ARGS"

	// Below keys are serverless related fileds
	LogKeyKeyspaceName        = "KEYSPACE_NAME"
	LogKeyServerlessTenantID  = "SERVERLESS_TENANT_ID"
	LogKeyServerlessProjectID = "SERVERLESS_PROJECT_ID"
	LogKeyServerlessClusterID = "SERVERLESS_CLUSTER_ID"

	// Below keys are copied form slow log, which are intended for performance tuning.
	// Infomations exposed by executor such as kv_total/retry_count can't be collected in audit log.
	LogKeyTimeTotal    = "QUERY_TIME"
	LogKeyTimeParse    = "PARSE_TIME"
	LogKeyTimeComile   = "COMPILE_TIME"
	LogKeyTimeOptimize = "OPTIMIZE_TIME"
	LogKeyTimeWaitTS   = "WAIT_TS"
	LogKeyIndexNames   = "INDEX_NAMES"
	LogKeyCopTasks     = "COP_TASKS"
	LogKeyExecDetail   = "EXEC_DETAIL"
	LogKeyMemMax       = "MEM_MAX"
	LogKeyDiskMax      = "DISK_MAX"
	LogKeyPrevStmt     = "PREV_STMT"
	LogKeyPlanDigest   = "PLAN_DIGEST"

	// Below keys are collected for debugging transaction
	LogKeyStartTime      = "START_TIME"
	LogKeyTxnCreateTime  = "TXN_CREATE_TIME"
	LogKeyTxnStartTS     = "TXN_START_TS"
	LogKeyTxnForUpdateTS = "TXN_FOR_UPDATE_TS"
)

var logFieldsPool = createLogFieldsPool(1024)

func createLogFieldsPool(capacity int) *pools.ResourcePool {
	return pools.NewResourcePool(
		func() (pools.Resource, error) {
			return newLogFields(), nil
		},
		capacity, capacity, 0,
	)
}

type logFields []zap.Field

func newLogFields() logFields {
	return make([]zap.Field, 0, 32)
}

func fieldsFromPool(pool *pools.ResourcePool) (logFields, func()) {
	resource, err := pool.TryGet()
	if err != nil {
		terror.Log(err)
		return newLogFields(), func() {}
	}

	if fields, ok := resource.(logFields); ok {
		return fields, func() {
			pool.Put(fields)
		}
	}

	return newLogFields(), func() {}
}

func (logFields) Close() {}

// LogEntry is the entry for the audit log
type LogEntry struct {
	user         string
	host         string
	roles        []*auth.RoleIdentity
	connID       uint64
	gwConnID     string
	classes      []EventClass
	tables       []stmtctx.TableEntry
	err          error
	keyspaceName string
	fieldsFunc   func(fields []zap.Field, cfg *LoggerConfig) []zap.Field
}

func serverlessFields() []zap.Field {
	return []zap.Field{
		zap.String(LogKeyServerlessTenantID, metrics.ServerlessTenantID),
		zap.String(LogKeyServerlessProjectID, metrics.ServerlessProjectID),
		zap.String(LogKeyServerlessClusterID, metrics.ServerlessClusterID),
	}
}

func newAuditEntryWithSetGlobalVar(name, value string, sessVars *variable.SessionVars, err error) *LogEntry {
	connInfo := sessVars.ConnectionInfo
	if connInfo == nil {
		// if connInfo is nil, it means it is a background setting, only log this event when user manually set it.
		return nil
	}

	var classes []EventClass
	switch name {
	case TiDBAuditEnabled:
		var auditEnableClass EventClass
		if variable.TiDBOptOn(value) {
			auditEnableClass = ClassAuditEnable
		} else {
			auditEnableClass = ClassAuditDisable
		}
		classes = []EventClass{ClassAudit, ClassAuditSetSysVar, auditEnableClass}
	default:
		classes = []EventClass{ClassAudit, ClassAuditSetSysVar}
	}

	return &LogEntry{
		user:         connInfo.User,
		host:         connInfo.Host,
		roles:        sessVars.ActiveRoles,
		connID:       connInfo.ConnectionID,
		gwConnID:     connInfo.GwConnID,
		classes:      classes,
		err:          err,
		keyspaceName: keyspace.GetKeyspaceNameBySettings(),
		fieldsFunc: func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
			fields = append(fields, serverlessFields()...)
			return append(
				fields,
				zap.String(LogKeyAuditOpTarget, name),
				zap.Strings(LogKeyAuditOpArgs, []string{value}),
			)
		},
	}
}

func newAuditFuncCallEntry(name string, args []types.Datum, ctx extension.FunctionContext, err error) *LogEntry {
	var connID uint64
	var gwConnID, user, host string
	if conn := ctx.ConnectionInfo(); conn != nil {
		connID = conn.ConnectionID
		gwConnID = conn.GwConnID
		user = conn.User
		host = conn.Host
	}

	return &LogEntry{
		user:         user,
		host:         host,
		roles:        ctx.ActiveRoles(),
		connID:       connID,
		gwConnID:     gwConnID,
		err:          err,
		keyspaceName: keyspace.GetKeyspaceNameBySettings(),
		classes:      []EventClass{ClassAudit, ClassAuditFuncCall},
		fieldsFunc: func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
			fields = append(fields, serverlessFields()...)
			return append(
				fields,
				zap.String(LogKeyAuditOpTarget, name),
				zap.Strings(LogKeyAuditOpArgs, toStringArgs(args)),
			)
		},
	}
}

func newConnEventEntry(tp extension.ConnEventTp, info *extension.ConnEventInfo) *LogEntry {
	e := &LogEntry{
		user:         info.User,
		host:         info.Host,
		roles:        info.ActiveRoles,
		connID:       info.ConnectionID,
		gwConnID:     info.GwConnID,
		err:          info.Error,
		keyspaceName: keyspace.GetKeyspaceNameBySettings(),
	}

	switch tp {
	case extension.ConnHandshakeAccepted, extension.ConnHandshakeRejected:
		e.classes = []EventClass{ClassConnection, ClassConnect}
	case extension.ConnReset:
		e.classes = []EventClass{ClassConnection, ClassChangeUser}
	case extension.ConnDisconnected:
		e.classes = []EventClass{ClassConnection, ClassDisconnect}
	default:
		return nil
	}

	e.user = info.User
	e.host = info.Host
	e.roles = info.ActiveRoles
	e.connID = info.ConnectionID
	e.gwConnID = info.GwConnID
	e.err = info.Error
	e.fieldsFunc = func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
		fields = append(fields, serverlessFields()...)
		if tp != extension.ConnDisconnected {
			// only log current db when connect or change user
			currentDB, _ := encryptIfKeySet(info.DB, cfg.EncryptKey, cfg.EncryptIv)
			fields = append(fields, zap.String(LogKeyCurrentDB, currentDB))
		}
		return append(
			fields,
			zap.String(LogKeyConnectionTYPE, info.ConnectionType),
			zap.String(LogKeyPID, strconv.Itoa(info.PID)),
			zap.String(LogKeyServerVersion, info.ServerVersion),
			zap.String(LogKeySSLVersion, info.SSLVersion),
			zap.String(LogKeyHostIP, normalizeIP(info.ServerIP)),
			zap.String(LogKeyHostPort, strconv.Itoa(info.ServerPort)),
			zap.String(LogKeyClientIP, normalizeIP(info.ClientIP)),
			zap.String(LogKeyClientPort, info.ClientPort),
		)
	}

	return e
}

// Statements containing password should always be masked.
func isPasswordStmt(node ast.StmtNode) bool {
	if node == nil {
		return false
	}

	findInSpecs := func(specs []*ast.UserSpec) bool {
		for _, spec := range specs {
			if authOpt := spec.AuthOpt; authOpt != nil && (authOpt.ByAuthString || authOpt.ByHashString) {
				return true
			}
		}
		return false
	}

	switch stmt := node.(type) {
	case *ast.SetPwdStmt:
		return true
	case *ast.CreateUserStmt:
		return findInSpecs(stmt.Specs)
	case *ast.AlterUserStmt:
		return findInSpecs(stmt.Specs)
	}
	return false
}

func newStmtEventEntry(tp extension.StmtEventTp, info extension.StmtEventInfo) *LogEntry {
	if tp != extension.StmtSuccess && tp != extension.StmtError {
		return nil
	}

	isExecute := info.ExecuteStmtNode() != nil
	innerStmtNode := info.ExecutePreparedStmt()
	if innerStmtNode == nil {
		innerStmtNode = info.StmtNode()
	}

	entry := &LogEntry{
		classes:      getStmtClasses(innerStmtNode, isExecute),
		tables:       info.RelatedTables(),
		err:          info.GetError(),
		keyspaceName: keyspace.GetKeyspaceNameBySettings(),
	}

	if connInfo := info.ConnectionInfo(); connInfo != nil {
		entry.connID = connInfo.ConnectionID
		entry.gwConnID = connInfo.GwConnID
		entry.user = connInfo.User
		entry.host = connInfo.Host
		entry.roles = info.ActiveRoles()
	}
	entry.fieldsFunc = func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
		var sqlText string
		if cfg.Redact || isPasswordStmt(info.StmtNode()) {
			sqlText, _ = info.SQLDigest()
		} else {
			sqlText = info.OriginalText()
		}
		sqlText, _ = encryptIfKeySet(sqlText, cfg.EncryptKey, cfg.EncryptIv)

		currentDB, _ := encryptIfKeySet(info.CurrentDB(), cfg.EncryptKey, cfg.EncryptIv)

		fields = append(fields, serverlessFields()...)
		fields = append(
			fields,
			zap.String(LogKeyCurrentDB, currentDB),
			zap.String(LogKeySQLText, sqlText),
		)

		if sessVars := info.GetSessionVars(); sessVars != nil {
			_, planDigest := sessVars.StmtCtx.GetPlanDigest()
			var digest string
			if planDigest != nil {
				digest, _ = encryptIfKeySet(planDigest.String(), cfg.EncryptKey, cfg.EncryptIv)
			}

			indexNames := make([]string, len(sessVars.StmtCtx.IndexNames))
			for i, name := range sessVars.StmtCtx.IndexNames {
				indexNames[i], _ = encryptIfKeySet(name, cfg.EncryptKey, cfg.EncryptIv)
			}

			fields = append(
				fields,
				zap.String(LogKeyTimeTotal, strconv.FormatFloat((time.Since(sessVars.StartTime)+sessVars.DurationParse).Seconds(), 'f', -1, 64)),
				zap.String(LogKeyTimeParse, strconv.FormatFloat(sessVars.DurationParse.Seconds(), 'f', -1, 64)),
				zap.String(LogKeyTimeComile, strconv.FormatFloat(sessVars.DurationCompile.Seconds(), 'f', -1, 64)),
				zap.String(LogKeyTimeOptimize, strconv.FormatFloat(sessVars.DurationOptimization.Seconds(), 'f', -1, 64)),
				zap.String(LogKeyTimeWaitTS, strconv.FormatFloat(sessVars.DurationWaitTS.Seconds(), 'f', -1, 64)),
				zap.Strings(LogKeyIndexNames, indexNames),
				zap.Any(LogKeyCopTasks, sessVars.StmtCtx.CopTasksDetails()),
				zap.Any(LogKeyExecDetail, sessVars.StmtCtx.GetExecDetails()),
				zap.Int64(LogKeyMemMax, sessVars.MemTracker.MaxConsumed()),
				zap.Int64(LogKeyDiskMax, sessVars.DiskTracker.MaxConsumed()),
				zap.String(LogKeyPlanDigest, digest),
				zap.Time(LogKeyStartTime, sessVars.StartTime),
			)

			if sessVars.PrevStmt != nil {
				prevStmt, _ := encryptIfKeySet(sessVars.PrevStmt.String(), cfg.EncryptKey, cfg.EncryptIv)
				fields = append(fields, zap.String(LogKeyPrevStmt, prevStmt))
			}

			if sessVars.TxnCtx != nil {
				fields = append(
					fields,
					zap.Time(LogKeyTxnCreateTime, sessVars.TxnCtx.CreateTime),
					zap.Uint64(LogKeyTxnStartTS, sessVars.TxnCtx.StartTS),
					zap.Uint64(LogKeyTxnForUpdateTS, sessVars.TxnCtx.GetForUpdateTS()),
				)
			}
		}

		if isExecute && !cfg.Redact {
			fields = append(fields, zap.Strings(LogKeyExecuteParams, toStringArgs(info.PreparedParams())))
		}

		for _, class := range entry.classes {
			if class == ClassDML {
				fields = append(fields, zap.Uint64(LogKeyAffectedRows, info.AffectedRows()))
				break
			}
		}

		return fields
	}
	return entry
}

// Filter filters this log entry by filter
func (e *LogEntry) Filter(filter *LogFilterRuleBundle) *LogEntry {
	return filter.Filter(e)
}

// Log logs this entry
func (e *LogEntry) Log(logger *Logger, id *entryIDGenerator) {
	if e == nil {
		return
	}

	events := make([]string, 0, len(e.classes))
	for _, class := range e.classes {
		if class.displayInLog() {
			events = append(events, class.String())
		}
	}

	var roles []string
	if len(e.roles) > 0 {
		roles = make([]string, len(e.roles))
		for i, role := range e.roles {
			roles[i] = role.String()
		}
	}

	var tables []string
	if len(e.tables) > 0 {
		tables = make([]string, len(e.tables))
		for i, tb := range e.tables {
			tables[i], _ = encryptIfKeySet(fmt.Sprintf("`%s`.`%s`", tb.DB, tb.Table), logger.cfg.EncryptKey, logger.cfg.EncryptIv)
		}
	}

	user, _ := encryptIfKeySet(e.user, logger.cfg.EncryptKey, logger.cfg.EncryptIv)

	fields, closeFields := fieldsFromPool(logFieldsPool)
	defer closeFields()

	fields = append(
		fields,
		zap.String(LogKeyID, id.Next()),
		zap.Strings(LogKeyEvent, events),
		zap.String(LogKeyUser, user),
		zap.Strings(LogKeyRoles, roles),
		zap.String(LogKeyConnectionID, strconv.FormatUint(e.connID, 10)),
		zap.String(LogKeyGwConnID, e.gwConnID),
		zap.Strings(LogKeyTables, tables),
		zap.Int(LogKeyStatusCode, getStatusCode(e.err)),
		zap.String(LogKeyKeyspaceName, e.keyspaceName),
	)

	if err := e.err; err != nil {
		fields = append(fields, zap.String(LogKeyReason, err.Error()))
	}

	if e.fieldsFunc != nil {
		fields = e.fieldsFunc(fields, &logger.cfg)
	}

	logger.Event(fields)
}

func getStatusCode(err error) int {
	if err != nil {
		return 0
	}
	return 1
}

// Encrypt text to base64 string with AES-256, if the key if empty, return the original text
func encryptIfKeySet(text, key, iv string) (string, error) {
	if key == "" {
		return text, nil
	}
	encrypted, err := encrypt.AESEncryptWithCBC([]byte(text), []byte(key), []byte(iv))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encrypted), nil
}
