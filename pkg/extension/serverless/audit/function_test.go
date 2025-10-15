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

package audit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
)

func eventClasses2String(eventClasses []EventClass) string {
	eventClassNames := make([]string, len(eventClasses))
	for i, eventClass := range eventClasses {
		eventClassNames[i] = class2String[eventClass]
	}
	return fmt.Sprintf(`[EVENT="[%s]"]`, strings.Join(eventClassNames, ","))
}

func TestAuditLogRotate(t *testing.T) {
	register4Test()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	fileName := "tidb-audit-log-rotate"
	logName := fmt.Sprintf("%s.log", fileName)
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", logName))

	_, err := deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)

	tk.MustExec("SET global tidb_audit_log_reserved_backups = 2")
	tk.MustExec("SET global tidb_audit_enabled = 1")
	for i := 0; i < 5; i++ {
		tk.MustQuery("SELECT audit_log_rotate()").Check(testkit.Rows("OK"))
		time.Sleep(time.Second)
	}

	files, err := deleteAllAuditLogs(workDir, fmt.Sprintf("%s-", fileName), ".log")
	require.NoError(t, err)
	require.Equal(t, 2, len(files), files)
}

func TestAuditLogEnableRule(t *testing.T) {
	register4Test()
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)

	fileName := "tidb-audit-admin"
	logName := fmt.Sprintf("%s.log", fileName)
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", logName))

	tk.MustExec("SET global tidb_audit_enabled = 1")
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, DefAuditLogName)))

	_, err = deleteAllAuditLogs(tempDir, fileName, ".log")
	require.NoError(t, err)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustQuery("select audit_log_create_filter('all', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select content from mysql.audit_log_filters where filter_name = 'all'").Check(testkit.Rows(`{}`))
	tk.MustQuery("select audit_log_create_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	tk.MustQuery("select enabled from mysql.audit_log_filter_rules where user = '%@%' and filter_name = 'all'").Check(testkit.Rows("1"))
	tk.MustExec("SET global tidb_audit_log_redacted = 0")

	require.NoError(t, conn.HandleQuery(context.Background(), "use test"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into t values (1)"))
	ok, _, err := containsMessage(filepath.Join(tempDir, DefAuditLogName), "insert into t values (1)")
	require.NoError(t, err)
	require.True(t, ok)

	tk.MustQuery("select audit_log_disable_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into t values (2)"))
	ok, _, err = containsMessage(filepath.Join(tempDir, DefAuditLogName), "insert into t values (2)")
	require.NoError(t, err)
	require.False(t, ok)

	tk.MustQuery("select audit_log_enable_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into t values (3)"))
	ok, _, err = containsMessage(filepath.Join(tempDir, DefAuditLogName), "insert into t values (3)")
	require.NoError(t, err)
	require.True(t, ok)

	_, err = deleteAllAuditLogs(tempDir, fileName, ".log")
	require.NoError(t, err)
}

func TestAuditAdmin(t *testing.T) {
	register4Test()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)

	fileName := "tidb-audit-restricted-admin"
	logName := fmt.Sprintf("%s.log", fileName)
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", logName))

	tk.MustExec("CREATE USER testuser")
	tk2 := testkit.NewTestKit(t, store)
	err = tk2.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	tk.MustExec("GRANT SELECT, SYSTEM_VARIABLES_ADMIN ON *.* to testuser")

	tk2.MustGetErrCode(`select audit_log_create_filter("empty", '{"filter":[]}')`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`set global tidb_audit_enabled = 0`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`select * from mysql.audit_log_filters`, errno.ErrTableaccessDenied)

	tk.MustExec("GRANT AUDIT_ADMIN ON *.* to testuser")

	tk2.MustQuery(`select audit_log_create_filter("all_query", '{"filter":[{"class":["QUERY"]}]}')`).Check(testkit.Rows("OK"))
	tk2.MustExec(`set global tidb_audit_enabled = 0`)
	tk2.MustQuery(`select * from mysql.audit_log_filters`).Check(testkit.Rows(`all_query {"filter":[{"class":["QUERY"]}]}`))

	_, err = deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)
}

func TestRestrictedAuditAdmin(t *testing.T) {
	register4Test()
	store := testkit.CreateMockStore(t)
	sem.Enable(config.SEMLevelBasic)
	defer sem.Disable()
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)

	fileName := "tidb-audit-restricted-admin"
	logName := fmt.Sprintf("%s.log", fileName)
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", logName))

	tk.MustExec("CREATE USER testuser")
	tk2 := testkit.NewTestKit(t, store)
	err := tk2.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	tk.MustExec("GRANT SELECT, SYSTEM_VARIABLES_ADMIN, AUDIT_ADMIN ON *.* to testuser")

	tk2.MustGetErrCode(`select audit_log_create_filter("empty", '{"filter":[]}')`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`set global tidb_audit_enabled = 0`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`select * from mysql.audit_log_filters`, errno.ErrTableaccessDenied)

	tk.MustExec("GRANT RESTRICTED_AUDIT_ADMIN ON *.* to testuser")

	tk2.MustQuery(`select audit_log_create_filter("all_query", '{"filter":[{"class":["QUERY"]}]}')`).Check(testkit.Rows("OK"))
	tk2.MustExec(`set global tidb_audit_enabled = 0`)
	tk2.MustQuery(`select * from mysql.audit_log_filters`).Check(testkit.Rows(`all_query {"filter":[{"class":["QUERY"]}]}`))

	_, err = deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)
}

func TestEventClass(t *testing.T) {
	register4Test()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	fileName := "tidb-audit-event-class"
	logName := fmt.Sprintf("%s.log", fileName)
	defer func() {
		_, err := deleteAllAuditLogs(workDir, fileName, ".log")
		require.NoError(t, err)
	}()
	logPath := filepath.Join(workDir, logName)
	_, err := deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)
	require.NoError(t, conn.HandleQuery(context.Background(), fmt.Sprintf("SET global tidb_audit_log = '%s'", logName)))
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_filter('all', '{}')"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_rule('%@%', 'all')"))

	// test QUERY and AUDIT
	testcases := []struct {
		sql           string
		requireEvents []EventClass
		success       bool
	}{
		{"use test", []EventClass{ClassQuery}, true},
		{"begin", []EventClass{ClassQuery, ClassTransaction}, true},
		{"create table if not exists t (c int)", []EventClass{ClassQuery, ClassDDL}, true},
		{"select * from t", []EventClass{ClassQuery, ClassSelect}, true},
		{"commit", []EventClass{ClassQuery, ClassTransaction}, true},
		{"prepare abs_func from 'select abs(?)'", []EventClass{ClassQuery}, true},
		{"set @num = -1", []EventClass{ClassQuery}, true},
		{"execute abs_func using @num", []EventClass{ClassQuery, ClassExecute, ClassSelect}, true},
		{"insert into t values (1)", []EventClass{ClassQuery, ClassDML, ClassInsert}, true},
		{"replace into t values (2)", []EventClass{ClassQuery, ClassDML, ClassReplace}, true},
		{"update t set c = 3 where c = 1", []EventClass{ClassQuery, ClassDML, ClassUpdate}, true},
		{"delete from t", []EventClass{ClassQuery, ClassDML, ClassDelete}, true},
		{`LOAD DATA LOCAL INFILE 'load_data.csv' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n'`, []EventClass{ClassQuery, ClassDML, ClassLoadData}, false},
		{"select audit_log_create_filter('empty', '{}')", []EventClass{ClassAudit, ClassAuditFuncCall}, true},
		{"set global tidb_audit_enabled = 1", []EventClass{ClassAudit, ClassAuditSetSysVar, ClassAuditEnable}, true},
		{"set global tidb_audit_enabled = 0", []EventClass{ClassAudit, ClassAuditSetSysVar, ClassAuditDisable}, true},
	}
	for _, tc := range testcases {
		if tc.success {
			require.NoError(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		} else {
			require.Error(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		}
		eventClassesStr := eventClasses2String(tc.requireEvents)
		ok, log, err := containsMessage(logPath, eventClassesStr)
		require.NoError(t, err, "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		require.True(t, ok, "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_rotate()"), "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		time.Sleep(time.Second)
	}
}

func lastLineContainsMessage(logpath, msg string) (ok bool, str string, err error) {
	bytes, err := os.ReadFile(logpath)
	strs := strings.Split(string(bytes), "\n")
	if len(strs) >= 2 {
		str = strs[len(strs)-2]
		if err != nil {
			return false, str, err
		}
	}
	return strings.Contains(str, msg), str, nil
}

func TestServerlessDisabled(t *testing.T) {
	fileName := "audit-serverless-disabled"
	conf := config.AuditLog{Path: fmt.Sprintf("%s.log", fileName)}

	registerForServerlessTest(&conf)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	defer func() {
		_, err := deleteAllAuditLogs(workDir, fileName, ".log")
		require.NoError(t, err)
	}()
	logPath := filepath.Join(workDir, conf.Path)
	_, err := deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)

	sqls := []string{
		"use test",
		"begin",
		"create table if not exists t (c int)",
		"select * from t",
		"commit",
	}
	for _, sql := range sqls {
		require.NoError(t, conn.HandleQuery(context.Background(), sql), sql)
	}

	_, err = os.ReadFile(logPath)
	require.Error(t, err, "audit log file should not exists if audit log disabled")
}

func TestServerlessEnabled(t *testing.T) {
	fileName := "tidb-audit-serverless-enabled"
	conf := config.AuditLog{
		Path:        fmt.Sprintf("%s.log", fileName),
		Format:      LogFormatText,
		MaxFilesize: 10,
		MaxLifetime: 60 * 60,
	}
	registerForServerlessTest(&conf)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)

	defer func() {
		_, err := deleteAllAuditLogs(workDir, fileName, ".log")
		require.NoError(t, err)
	}()
	logPath := filepath.Join(workDir, conf.Path)
	_, err := deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)

	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	tk.MustQuery("select audit_log_create_filter('all', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select content from mysql.audit_log_filters where filter_name = 'all'").Check(testkit.Rows(`{}`))
	tk.MustQuery("select audit_log_create_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	tk.MustQuery("select enabled from mysql.audit_log_filter_rules where user = '%@%' and filter_name = 'all'").Check(testkit.Rows("1"))

	testcases := []struct {
		sql           string
		requireEvents []EventClass
		success       bool
	}{
		{"use test", []EventClass{ClassQuery}, true},
		{"begin", []EventClass{ClassQuery, ClassTransaction}, true},
		{"create table if not exists t (c int)", []EventClass{ClassQuery, ClassDDL}, true},
		{"select * from t", []EventClass{ClassQuery, ClassSelect}, true},
		{"commit", []EventClass{ClassQuery, ClassTransaction}, true},
		{"insert into t values (1)", []EventClass{ClassQuery, ClassDML, ClassInsert}, true},
		{"replace into t values (2)", []EventClass{ClassQuery, ClassDML, ClassReplace}, true},
		{"update t set c = 3 where c = 1", []EventClass{ClassQuery, ClassDML, ClassUpdate}, true},
		{"delete from t", []EventClass{ClassQuery, ClassDML, ClassDelete}, true},
	}
	for _, tc := range testcases {
		if tc.success {
			require.NoError(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		} else {
			require.Error(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		}
		eventClassesStr := eventClasses2String(tc.requireEvents)
		ok, log, err := lastLineContainsMessage(logPath, eventClassesStr)
		require.NoError(t, err, "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		require.True(t, ok, "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		ok, log, err = lastLineContainsMessage(logPath, tc.sql)
		require.NoError(t, err, "%s\n%s", log, tc.sql)
		require.True(t, ok, "%s\n%s", log, tc.sql)
	}

	// Enable encryption to validate it works
	conf.EncryptKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 0"))
	registerForServerlessTest(&conf)
	for _, tc := range testcases {
		if tc.success {
			require.NoError(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		} else {
			require.Error(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		}
		ok, log, err := lastLineContainsMessage(logPath, tc.sql)
		require.NoError(t, err, "%s\n%s", log, tc.sql)
		require.False(t, ok, "%s\n%s", log, tc.sql)
	}
}

func TestServerlessSwitchFilterRule(t *testing.T) {
	fileName := "tidb-audit-serverless-filter-rule"
	conf := config.AuditLog{
		Path:        fmt.Sprintf("%s.log", fileName),
		Format:      LogFormatText,
		MaxFilesize: 10,
		MaxLifetime: 60 * 60,
	}
	registerForServerlessTest(&conf)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)

	defer func() {
		_, err := deleteAllAuditLogs(workDir, fileName, ".log")
		require.NoError(t, err)
	}()
	logPath := filepath.Join(workDir, conf.Path)
	_, err := deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)

	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustQuery("select audit_log_create_filter('all', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select content from mysql.audit_log_filters where filter_name = 'all'").Check(testkit.Rows(`{}`))
	tk.MustQuery("select audit_log_create_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	tk.MustQuery("select enabled from mysql.audit_log_filter_rules where user = '%@%' and filter_name = 'all'").Check(testkit.Rows("1"))

	sql := "insert into test.t values (1)"
	require.NoError(t, conn.HandleQuery(context.Background(), sql))
	ok, log, err := lastLineContainsMessage(logPath, sql)
	require.NoError(t, err, "%s\n%s", sql, log)
	require.True(t, ok, "%s\n%s", sql, log)

	tk.MustQuery("select audit_log_disable_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	sql = "insert into test.t values (2)"
	require.NoError(t, conn.HandleQuery(context.Background(), sql))
	ok, log, err = lastLineContainsMessage(logPath, sql)
	require.NoError(t, err, "%s\n%s", sql, log)
	require.False(t, ok, "%s\n%s", sql, log)

	tk.MustQuery("select audit_log_enable_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	sql = "insert into test.t values (3)"
	require.NoError(t, conn.HandleQuery(context.Background(), sql))
	ok, log, err = lastLineContainsMessage(logPath, sql)
	require.NoError(t, err, "%s\n%s", sql, log)
	require.True(t, ok, "%s\n%s", sql, log)
}
