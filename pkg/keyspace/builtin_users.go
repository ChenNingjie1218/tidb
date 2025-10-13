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

	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var (
	builtInUsers map[string]map[string]mysql.PrivilegeType
	initOnce     sync.Once
)

const (
	cloudAiUserName = "cloud_ai"
	cloudAiDb       = "ai"
)

func getBuiltInUserName(user string) string {
	keyspaceName := GetKeyspaceNameBySettings()
	if !IsKeyspaceNameEmpty(keyspaceName) {
		user = keyspaceName + "." + user
	}
	return user
}

// InitBuiltIn initializes builtin users idempotently
func InitBuiltIn() {
	initOnce.Do(func() {
		builtInUsers = map[string]map[string]mysql.PrivilegeType{
			getBuiltInUserName(cloudAiUserName): {cloudAiDb: mysql.SelectPriv | mysql.UpdatePriv | mysql.AlterPriv | mysql.IndexPriv},
		}
	})
}

// IsBuiltInUser check if a user is built-in
func IsBuiltInUser(user string) bool {
	_, ok := builtInUsers[user]
	return ok
}

// GetBuiltInUsers returns built-in users
func GetBuiltInUsers() []string {
	users := make([]string, 0, len(builtInUsers))
	for user := range builtInUsers {
		users = append(users, user)
	}
	return users
}

// GetBuiltInUserDBs returns privilege of databases for built-in users
func GetBuiltInUserDBs(user string) map[string]mysql.PrivilegeType {
	return builtInUsers[user]
}
