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
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type testError struct {
	orgErr   error
	tagedErr error
	tag      errTag
}

// TestErrTag tests that the error tag is added to the error message correctly.
func TestErrTag(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = map[string]string{
		"user-prefix-error":              "extend user prefix error message",
		"require-secure-transport-error": "extend require secure transport error message",
		"resource-unit-error":            "extend resource unit error message",
		"serverless-not-support-error":   "extend serverless not support error message",
		"invisible-table-error":          "extend invisible table error message",
		"invisible-sysvar-error":         "extend invisible sysvar error message",
	}
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	testErrs := make([]testError, 0, 20)

	for i := range 20 {
		testErr := testError{
			orgErr: errors.Errorf("test error %d", i),
			tag:    errTag(rand.Intn(8)),
		}
		switch testErr.tag {
		case errUserPrefix:
			testErr.tagedErr = WithUserPrefixErrTag(testErr.orgErr)
		case errRequireSecureTransport:
			testErr.tagedErr = WithRequireSecureTransportErrTag(testErr.orgErr)
		case errResourceUnit:
			testErr.tagedErr = WithResourceUnitErrTag(testErr.orgErr)
		case errServerlessNotSupport:
			testErr.tagedErr = WithServerlessNotSupportErrTag(testErr.orgErr)
		case errInvisibleTable:
			testErr.tagedErr = WithInvisibleTableErrTag(testErr.orgErr)
		case errInvisibleSysVar:
			testErr.tagedErr = WithInvisibleSysVarErrTag(testErr.orgErr)
		default:
			testErr.tagedErr = testErr.orgErr
		}
		testErrs = append(testErrs, testErr)
	}

	for i := range 20 {
		tag := testErrs[i].tag
		tagedErr := testErrs[i].tagedErr
		switch tag {
		case errUserPrefix:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errUserPrefix)))
		case errRequireSecureTransport:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errRequireSecureTransport)))
		case errResourceUnit:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errResourceUnit)))
		case errServerlessNotSupport:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errServerlessNotSupport)))
		case errInvisibleTable:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errInvisibleTable)))
		case errInvisibleSysVar:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errInvisibleSysVar)))
		default:
			assert.False(t, strings.HasPrefix(tagedErr.Error(), "WithErrTag:"))
		}
	}
}

// TestExtendErrorMessage tests the ExtendErrorMessage function that can extend the error message correctly.
func TestExtendErrorMessage(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = map[string]string{
		"user-prefix-error":              "extend user prefix error message",
		"require-secure-transport-error": "extend require secure transport error message",
		"resource-unit-error":            "extend resource unit error message",
		"serverless-not-support-error":   "extend serverless not support error message",
		"invisible-table-error":          "extend invisible table error message",
		"invisible-sysvar-error":         "extend invisible sysvar error message",
	}
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	for i := range 20 {
		testErr := testError{
			orgErr: errors.Errorf("test error %d", i),
			tag:    errTag(rand.Intn(8)),
		}
		switch testErr.tag {
		case errUserPrefix:
			testErr.tagedErr = WithUserPrefixErrTag(testErr.orgErr)
		case errRequireSecureTransport:
			testErr.tagedErr = WithRequireSecureTransportErrTag(testErr.orgErr)
		case errResourceUnit:
			testErr.tagedErr = WithResourceUnitErrTag(testErr.orgErr)
		case errServerlessNotSupport:
			testErr.tagedErr = WithServerlessNotSupportErrTag(testErr.orgErr)
		case errInvisibleTable:
			testErr.tagedErr = WithInvisibleTableErrTag(testErr.orgErr)
		case errInvisibleSysVar:
			testErr.tagedErr = WithInvisibleSysVarErrTag(testErr.orgErr)
		default:
			testErr.tagedErr = testErr.orgErr
		}
		m := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, testErr.orgErr.Error())
		ExtendErrorMessage(testErr.tagedErr, m)

		switch testErr.tag {
		case errUserPrefix:
			assert.True(t, strings.HasSuffix(m.Message, "extend user prefix error message."))
		case errRequireSecureTransport:
			assert.True(t, strings.HasSuffix(m.Message, "extend require secure transport error message."))
		case errResourceUnit:
			assert.True(t, strings.HasSuffix(m.Message, "extend resource unit error message."))
		case errServerlessNotSupport:
			assert.True(t, strings.HasSuffix(m.Message, "extend serverless not support error message."))
		case errInvisibleTable:
			assert.True(t, strings.HasSuffix(m.Message, "extend invisible table error message."))
		case errInvisibleSysVar:
			assert.True(t, strings.HasSuffix(m.Message, "extend invisible sysvar error message."))
		default:
			assert.Equal(t, m.Message, testErr.orgErr.Error())
		}
	}
}
