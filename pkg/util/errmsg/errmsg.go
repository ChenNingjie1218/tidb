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

package errmsg

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pkg/errors"
)

type errTag int

const (
	unknownErrTag errTag = iota
	errUserPrefix
	errMaxSleepSeconds
	errRequireSecureTransport
	errResourceUnit
	errServerlessNotSupport
	errInvisibleTable
	errInvisibleSysVar
	errMinTiFlashReplica
	errReadOnlySysVar
)

// The tag name is used to find the extended error message from config file.
// For example, the tag name of errUserPrefix is "user-prefix-error",
// if we want to extend the error message of errUserPrefix,
// we can add the following config to the config file:
// ```
// [extended-error-messages]
// user-prefix-error = "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details"
// ```
func (e errTag) errTagName() string {
	switch e {
	case errUserPrefix:
		return "user-prefix-error"
	case errMaxSleepSeconds:
		return "max-sleep-seconds-error"
	case errRequireSecureTransport:
		return "require-secure-transport-error"
	case errResourceUnit:
		return "resource-unit-error"
	case errServerlessNotSupport:
		return "serverless-not-support-error"
	case errInvisibleTable:
		return "invisible-table-error"
	case errInvisibleSysVar:
		return "invisible-sysvar-error"
	case errMinTiFlashReplica:
		return "min-tiflash-replica-error"
	case errReadOnlySysVar:
		return "read-only-sysvar-error"
	default:
		return "unknown"
	}
}

func withErrTag(err error, tag errTag) error {
	eems := config.GetGlobalConfig().ExtendedErrorMsgs
	if _, ok := eems[tag.errTagName()]; ok {
		return errors.WithMessagef(err, "WithErrTag:%d", tag)
	}
	return err
}

// WithUserPrefixErrTag is used to add a tag to the user prefix error.
func WithUserPrefixErrTag(err error) error {
	return withErrTag(err, errUserPrefix)
}

// WithMaxSleepSecondsErrTag is used to add a tag to the max sleep seconds error.
func WithMaxSleepSecondsErrTag(err error) error {
	return withErrTag(err, errMaxSleepSeconds)
}

// WithRequireSecureTransportErrTag is used to add a tag to the require secure transport error.
func WithRequireSecureTransportErrTag(err error) error {
	return withErrTag(err, errRequireSecureTransport)
}

// WithResourceUnitErrTag is used to add a tag to the resource unit error.
func WithResourceUnitErrTag(err error) error {
	return withErrTag(err, errResourceUnit)
}

// WithServerlessNotSupportErrTag is used to add a tag to the serverless not support error.
func WithServerlessNotSupportErrTag(err error) error {
	return withErrTag(err, errServerlessNotSupport)
}

// WithInvisibleTableErrTag is used to add a tag to the invisible table error.
func WithInvisibleTableErrTag(err error) error {
	return withErrTag(err, errInvisibleTable)
}

// WithInvisibleSysVarErrTag is used to add a tag to the invisible sysvar error.
func WithInvisibleSysVarErrTag(err error) error {
	return withErrTag(err, errInvisibleSysVar)
}

// WithMinTiFlashReplicaErrTag is used to add a tag to the min tiflash replica error.
func WithMinTiFlashReplicaErrTag(err error) error {
	return withErrTag(err, errMinTiFlashReplica)
}

// WithReadOnlySysVarErrTag is used to add a tag to the read only sysvar error.
func WithReadOnlySysVarErrTag(err error) error {
	return withErrTag(err, errReadOnlySysVar)
}

func getErrTag(err error) errTag {
	errStr := err.Error()
	errTag := unknownErrTag
	if strings.HasPrefix(errStr, "WithErrTag:") {
		fmt.Sscanf(errStr, "WithErrTag:%d", &errTag)
	}
	return errTag
}

// ExtendErrorMessage is used to extend the error message.
func ExtendErrorMessage(err error, m *mysql.SQLError) {
	eems := config.GetGlobalConfig().ExtendedErrorMsgs
	if len(eems) == 0 {
		return
	}
	et := getErrTag(err)
	if et == unknownErrTag {
		return
	}
	if eem, ok := eems[et.errTagName()]; ok {
		extendErrorMessage(m, eem)
	}
}

func extendErrorMessage(m *mysql.SQLError, msg string) {
	m.Message = fmt.Sprintf("%s, %s.", strings.TrimSuffix(m.Message, "."), msg)
}
