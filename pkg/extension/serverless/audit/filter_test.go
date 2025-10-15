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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type entryFilterTest struct {
	entry LogEntry
	match bool
}

func TestFilterSpec(t *testing.T) {
	register4Test()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	fileName := "tidb-audit-filter-spec"
	logName := fmt.Sprintf("%s.log", fileName)
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", logName))

	cases := []struct {
		spec        filterSpec
		invalid     bool
		entries     []entryFilterTest
		jsonContent string
	}{
		{
			spec: filterSpec{
				Classes: []string{"abc"},
			},
			invalid: true,
		},
		{
			spec: filterSpec{
				ClassesExclude: []string{"abc"},
			},
			invalid: true,
		},
		{
			spec: filterSpec{
				Tables: []string{"t1"},
			},
			invalid: true,
		},
		{
			spec: filterSpec{
				TablesExclude: []string{"t1"},
			},
			invalid: true,
		},
		{
			// empty filterSpec can pass all types of log
			spec:        filterSpec{},
			jsonContent: "{}",
			entries: []entryFilterTest{
				{
					entry: LogEntry{},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery}},
					match: true,
				},
				{
					entry: LogEntry{
						user:    "u1",
						classes: []EventClass{ClassQuery},
						tables: []stmtctx.TableEntry{
							{DB: "db1", Table: "t1"},
							{DB: "db2", Table: "t2"},
						},
					},
					match: true,
				},
				{
					entry: LogEntry{
						err: errors.New(""),
					},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				Classes: []string{"QUERY_DML", "QUERY_DDL"},
			},
			jsonContent: `{"class": ["QUERY_DML", "QUERY_DDL"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery, ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery, ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery, ClassSelect}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", err: errors.New(""), classes: []EventClass{ClassQuery, ClassDML}, tables: []stmtctx.TableEntry{
						{DB: "db1", Table: "t1"},
						{DB: "db2", Table: "t2"},
					}},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				ClassesExclude: []string{"SELECT", "INSERT"},
			},
			jsonContent: `{"class_excl": ["SELECT", "INSERT"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery, ClassSelect}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery, ClassInsert}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassQuery, ClassUpdate}},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				Classes:        []string{"QUERY_DDL", "QUERY_DML"},
				ClassesExclude: []string{"SELECT", "INSERT"},
			},
			jsonContent: `{"class": ["QUERY_DDL", "QUERY_DML"], "class_excl": ["SELECT", "INSERT"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassUpdate}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassUpdate}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassInsert}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassSelect}},
					match: false,
				},
			},
		},
		{
			spec: filterSpec{
				Classes:        []string{"SELECT", "QUERY_DML"},
				ClassesExclude: []string{"SELECT", "INSERT"},
			},
			jsonContent: `{"class": ["SELECT", "QUERY_DML"], "class_excl": ["SELECT", "INSERT"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDDL}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassUpdate}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassUpdate}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassInsert}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassSelect}},
					match: false,
				},
			},
		},
		{
			spec: filterSpec{
				Tables: []string{"*.*"},
			},
			jsonContent: `{"table": ["*.*"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "test", Table: "t1"},
					}},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				Tables: []string{"db*.test*", "test.t1"},
			},
			jsonContent: `{"table": ["db*.test*", "test.t1"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "db1", Table: "test1"},
					}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", err: errors.New(""), tables: []stmtctx.TableEntry{
						{DB: "db2", Table: "test2"},
					}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "test", Table: "t1"},
					}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "db1", Table: "t1"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "d1", Table: "test1"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "test", Table: "test1"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "db2", Table: "test2"},
					}},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				TablesExclude: []string{"db*.test*", "test.t1"},
			},
			jsonContent: `{"table_excl": ["db*.test*", "test.t1"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "db2", Table: "test2"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "test", Table: "t1"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "test", Table: "t2"},
					}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "d2", Table: "t2"},
						{DB: "db2", Table: "t2"},
					}},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				Tables:        []string{"db*.test*"},
				TablesExclude: []string{"db2.test*"},
			},
			jsonContent: `{"table": ["db*.test*"], "table_excl": ["db2.test*"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "db2", Table: "test2"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "db3", Table: "test1"},
					}},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				Tables:        []string{"db*.test*"},
				TablesExclude: []string{"db*.test*"},
			},
			jsonContent: `{"table": ["db*.test*"], "table_excl": ["db*.test*"]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "db2", Table: "test2"},
					}},
					match: false,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{
						{DB: "db3", Table: "test1"},
					}},
					match: false,
				},
			},
		},
		{
			spec: filterSpec{
				StatusCodes: []int{0},
			},
			jsonContent: `{"status_code": [0]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", err: errors.New("err")},
					match: true,
				},
			},
		},
		{
			spec: filterSpec{
				StatusCodes: []int{1},
			},
			jsonContent: `{"status_code": [1]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", err: errors.New("err")},
					match: false,
				},
			},
		},
		{
			spec: filterSpec{
				StatusCodes: []int{0, 1},
			},
			jsonContent: `{"status_code": [0, 1]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", err: errors.New("err")},
					match: true,
				},
			},
		},
	}

	for _, c := range cases {
		// test filter
		bs, err := json.Marshal(c.spec)
		require.NoError(t, err)
		specJSON := string(bs)
		if c.invalid {
			require.Error(t, c.spec.Validate(), specJSON)
			continue
		}
		require.NoError(t, c.spec.Validate(), specJSON)
		filter := logFilter{Name: "test", Filter: []filterSpec{c.spec}}
		fn := filter.createFilterFunc()
		for _, ent := range c.entries {
			require.Equal(t, ent.match, fn(&ent.entry), "%v %v", specJSON, ent.entry)
		}

		// test creating filter by sql
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_create_filter('test', '{"filter":[%s]}')`, c.jsonContent)).Check(testkit.Rows("OK"))
		tk.MustQuery(`SELECT audit_log_create_rule('%@%','test')`).Check(testkit.Rows("OK"))
		tk.MustQuery("SELECT count(*) FROM mysql.audit_log_filters").Check(testkit.Rows("1"))
		tk.MustQuery("SELECT COALESCE(content->>'$.filter[0]', '{}') FROM mysql.audit_log_filters WHERE filter_name = 'test'").Check(testkit.Rows(c.jsonContent))
		rules, err := listFilterRules(context.Background(), tk.Session())
		require.NoError(t, err, specJSON)
		require.Equal(t, 1, len(rules), specJSON)
		require.Equal(t, c.spec, rules[0].filter.Filter[0], specJSON)
		require.Equal(t, "test", rules[0].filterName, specJSON)
		tk.MustQuery("SELECT audit_log_remove_rule('%@%','test')").Check(testkit.Rows("OK"))
		tk.MustQuery("SELECT audit_log_remove_filter('test')").Check(testkit.Rows("OK"))
	}
	_, err := deleteAllAuditLogs(workDir, fileName, ".log")
	require.NoError(t, err)
}

func TestFilterRule(t *testing.T) {
	register4Test()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	cases := []struct {
		rule        logFilterRule
		entries     []entryFilterTest
		jsonContent string
	}{
		{
			rule: logFilterRule{
				user:       "root",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "u1"},
					match: false,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "root",
				filterName: "f1",
				enabled:    false,
				filter: &logFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "u1"},
					match: false,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "root@127.0.0._",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "u1"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", host: "localhost"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", host: "127.0.0.1"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", host: "127.0.0.11"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", host: "%"},
					match: false,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "%@%",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name: "f1",
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "u1"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", host: "localhost"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", host: "127.0.0.1"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", host: "127.0.0.11"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", host: "%"},
					match: true,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "root",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name:   "f1",
					Filter: []filterSpec{},
				},
			},
			jsonContent: `{}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "u1"},
					match: false,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "root",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name: "f1",
					Filter: []filterSpec{
						// An LogEntry matches the Filter if only any filterSpec matches
						{
							Classes: []string{"QUERY_DML"},
						},
						{
							Classes: []string{"QUERY_DDL"},
						},
					},
				},
			},
			jsonContent: `{"filter":[{"class":["QUERY_DML"]},{"class":["QUERY_DDL"]}]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: false,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassConnection}},
					match: false,
				},
				{
					entry: LogEntry{user: "user1", classes: []EventClass{ClassDML}},
					match: false,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "root",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name: "f1",
					Filter: []filterSpec{
						{
							Classes: []string{"QUERY_DML"},
						},
						{
							ClassesExclude: []string{"QUERY_DML"},
						},
					},
				},
			},
			jsonContent: `{"filter":[{"class":["QUERY_DML"]},{"class_excl":["QUERY_DML"]}]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "user1", classes: []EventClass{ClassDML}},
					match: false,
				},
			},
		},
		{
			rule: logFilterRule{
				user:       "root",
				filterName: "f1",
				enabled:    true,
				filter: &logFilter{
					Name: "f1",
					Filter: []filterSpec{
						{
							Tables: []string{"test*.t*"},
						},
						{
							TablesExclude: []string{"test*.t*"},
						},
					},
				},
			},
			jsonContent: `{"filter":[{"table":["test*.t*"]},{"table_excl":["test*.t*"]}]}`,
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root"},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{{DB: "test", Table: "t"}}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", tables: []stmtctx.TableEntry{{DB: "test", Table: "c"}}},
					match: true,
				},
				{
					entry: LogEntry{user: "user1"},
					match: false,
				},
			},
		},
	}

	for i, c := range cases {
		// test rule
		fn := c.rule.createFilterFunc()
		for j, ent := range c.entries {
			require.Equal(t, ent.match, fn(&ent.entry), "case index: %d entry index: %d", i, j)
		}

		// test creating rule by sql
		fmt.Println(c.jsonContent)
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_create_filter('%s', '%s')`, c.rule.filterName, c.jsonContent)).Check(testkit.Rows("OK"))
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_create_rule('%s','%s')`, c.rule.user, c.rule.filterName)).Check(testkit.Rows("OK"))
		tk.MustQuery("SELECT count(*) FROM mysql.audit_log_filters").Check(testkit.Rows("1"))
		tk.MustQuery("SELECT count(*) FROM mysql.audit_log_filter_rules").Check(testkit.Rows("1"))
		tk.MustQuery(fmt.Sprintf("SELECT content FROM mysql.audit_log_filters WHERE filter_name = '%s'", c.rule.filterName)).Check(testkit.Rows(c.jsonContent))
		if !c.rule.enabled {
			tk.MustQuery(fmt.Sprintf(`SELECT audit_log_disable_rule('%s','%s')`, c.rule.user, c.rule.filterName)).Check(testkit.Rows("OK"))
		}
		rules, err := listFilterRules(context.Background(), tk.Session())
		require.NoError(t, err, c.jsonContent)
		require.Equal(t, 1, len(rules), c.jsonContent)
		if rules[0].filter != nil && len(rules[0].filter.Filter) > 0 {
			require.Equal(t, c.rule, *rules[0], c.jsonContent)
		}
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_remove_rule('%s','%s')`, c.rule.user, c.rule.filterName)).Check(testkit.Rows("OK"))
		tk.MustQuery(fmt.Sprintf(`SELECT audit_log_remove_filter('%s')`, c.rule.filterName)).Check(testkit.Rows("OK"))
	}
}

func TestFilterRuleBundle(t *testing.T) {
	cases := []struct {
		bundle  *LogFilterRuleBundle
		entries []entryFilterTest
	}{
		{
			bundle: newLogFilterRuleBundle([]*logFilterRule{
				{
					user:       "root",
					filterName: "f1",
					enabled:    true,
					filter: &logFilter{
						Name: "f1",
						Filter: []filterSpec{
							{Classes: []string{"query_ddl"}},
						},
					},
				},
				{
					user:       "root",
					filterName: "f2",
					enabled:    true,
					filter: &logFilter{
						Name: "f2",
						Filter: []filterSpec{
							{Classes: []string{"query_dml"}, ClassesExclude: []string{"query_ddl"}},
						},
					},
				},
				{
					user:       "u1",
					filterName: "f2",
					enabled:    true,
					filter: &logFilter{
						Name: "f2",
						Filter: []filterSpec{
							{Classes: []string{"query_dml"}},
						},
					},
				},
			}),
			entries: []entryFilterTest{
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassDML, ClassDDL}},
					match: true,
				},
				{
					entry: LogEntry{user: "root", classes: []EventClass{ClassConnection}},
					match: false,
				},
				{
					entry: LogEntry{user: "u1", classes: []EventClass{ClassDML}},
					match: true,
				},
				{
					entry: LogEntry{user: "u1", classes: []EventClass{ClassDDL}},
					match: false,
				},
			},
		},
	}

	for i, c := range cases {
		for j, ent := range c.entries {
			filtered := c.bundle.Filter(&ent.entry)
			if filtered != nil {
				require.Same(t, &ent.entry, filtered, "case index: %d entry index: %d", i, j)
			}
			require.Equal(t, ent.match, filtered != nil, "case index: %d entry index: %d", i, j)
		}
	}
}
