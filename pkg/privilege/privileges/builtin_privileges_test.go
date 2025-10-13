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

package privileges

import (
	"testing"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
)

func TestNewBuiltInUserRecord(t *testing.T) {
	user := "test_user"
	userRecord := newBuiltInUserRecord(user)

	assert.Equal(t, "%", userRecord.Host)
	assert.Equal(t, user, userRecord.User)
	assert.Equal(t, mysql.AuthTiDBAuthToken, userRecord.AuthPlugin)
	assert.Equal(t, "pingcap.com", userRecord.AuthTokenIssuer)
	assert.Equal(t, user+"@pingcap.com", userRecord.UserAttributesInfo.Email)
}

func TestNewBuiltInDbRecord(t *testing.T) {
	user := "test_user"
	db := "test_db"
	privileges := mysql.SelectPriv | mysql.UpdatePriv | mysql.AlterPriv | mysql.IndexPriv
	dbRecord := newBuiltInDbRecord(user, db, privileges)

	assert.Equal(t, "%", dbRecord.Host)
	assert.Equal(t, user, dbRecord.User)
	assert.Equal(t, privileges, dbRecord.Privileges)
	assert.Equal(t, db, dbRecord.DB)
}

func TestInitBuiltIn(t *testing.T) {
	initBuiltIn()

	userRecords := getBuiltInUserRecords()
	dbRecords := getBuiltInDbRecords()

	assert.NotEmpty(t, userRecords)
	assert.NotEmpty(t, dbRecords)

	for _, user := range keyspace.GetBuiltInUsers() {
		assert.Contains(t, userRecords, newBuiltInUserRecord(user))
		for db, privileges := range keyspace.GetBuiltInUserDBs(user) {
			assert.Contains(t, dbRecords, newBuiltInDbRecord(user, db, privileges))
		}
	}
}
