// Copyright 2024 PingCAP, Inc.
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

package privileges

import (
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

var (
	builtInUserRecords = make([]UserRecord, 0)
	builtInDbRecords   = make([]dbRecord, 0)
	initOnce           sync.Once
)

func newBuiltInUserRecord(user string) UserRecord {
	userRecord := UserRecord{
		baseRecord: baseRecord{
			Host: "%",
			User: user,
		},
		AuthPlugin:      mysql.AuthTiDBAuthToken,
		AuthTokenIssuer: "pingcap.com",
		UserAttributesInfo: UserAttributesInfo{
			MetadataInfo: MetadataInfo{
				Email: user + "@pingcap.com",
			},
		},
	}
	userRecord.patChars, userRecord.patTypes = stringutil.CompilePatternBytes(userRecord.Host, '\\')
	userRecord.hostIPNet = parseHostIPNet(userRecord.Host)
	return userRecord
}

func newBuiltInDbRecord(user, db string, privileges mysql.PrivilegeType) dbRecord {
	dbRecord := dbRecord{
		baseRecord: baseRecord{
			Host: "%",
			User: user,
		},
		Privileges: privileges,
		DB:         db,
	}
	dbRecord.patChars, dbRecord.patTypes = stringutil.CompilePatternBytes(dbRecord.Host, '\\')
	dbRecord.hostIPNet = parseHostIPNet(dbRecord.Host)
	dbRecord.dbPatChars, dbRecord.dbPatTypes = stringutil.CompilePatternBytes(strings.ToUpper(dbRecord.DB), '\\')
	return dbRecord
}

func initBuiltIn() {
	initOnce.Do(func() {
		keyspace.InitBuiltIn()
		for _, user := range keyspace.GetBuiltInUsers() {
			builtInUserRecords = append(builtInUserRecords, newBuiltInUserRecord(user))
			for db, privileges := range keyspace.GetBuiltInUserDBs(user) {
				builtInDbRecords = append(builtInDbRecords, newBuiltInDbRecord(user, db, privileges))
			}
		}
	})
}

func getBuiltInUserRecords() []UserRecord {
	return builtInUserRecords
}

func getBuiltInDbRecords() []dbRecord {
	return builtInDbRecords
}
