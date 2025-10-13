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

package keyspace

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
)

func setKeyspaceName(keyspaceName string) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = keyspaceName
	})
}

func clearKeyspaceName() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = ""
	})
}

func TestGetBuiltInUserName(t *testing.T) {
	user := "test_user"
	setKeyspaceName("test_keyspace")
	assert.Equal(t, "test_keyspace.test_user", getBuiltInUserName(user))

	clearKeyspaceName()
	assert.Equal(t, user, getBuiltInUserName(user))
}

func TestIsBuiltInUser(t *testing.T) {
	setKeyspaceName("test_keyspace_2")
	initOnce = sync.Once{}
	InitBuiltIn()
	assert.True(t, IsBuiltInUser("test_keyspace_2.cloud_ai"))
	assert.False(t, IsBuiltInUser("cloud_ai"))

	clearKeyspaceName()
	initOnce = sync.Once{}
	InitBuiltIn()
	assert.True(t, IsBuiltInUser("cloud_ai"))
	assert.False(t, IsBuiltInUser("test_keyspace_2.cloud_ai"))
}

func TestGetBuiltInUsers(t *testing.T) {
	setKeyspaceName("test_keyspace_3")
	initOnce = sync.Once{}
	InitBuiltIn()
	assert.ElementsMatch(t, []string{"test_keyspace_3.cloud_ai"}, GetBuiltInUsers())

	clearKeyspaceName()
	initOnce = sync.Once{}
	InitBuiltIn()
	assert.ElementsMatch(t, []string{"cloud_ai"}, GetBuiltInUsers())
}

func TestGetBuiltInUserDBs(t *testing.T) {
	expected := map[string]mysql.PrivilegeType{"ai": mysql.SelectPriv | mysql.UpdatePriv | mysql.AlterPriv | mysql.IndexPriv}
	setKeyspaceName("test_keyspace_4")
	initOnce = sync.Once{}
	InitBuiltIn()
	assert.Equal(t, expected, GetBuiltInUserDBs("test_keyspace_4.cloud_ai"))

	clearKeyspaceName()
	initOnce = sync.Once{}
	InitBuiltIn()
	assert.Equal(t, expected, GetBuiltInUserDBs("cloud_ai"))
}
