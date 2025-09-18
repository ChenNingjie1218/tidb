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

package keyspace

import (
	"strings"

	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/errmsg"
)

// UsernamePolicy is the interface for username policy.
type UsernamePolicy interface {
	// ValidateUsername checks if the username is valid.
	ValidateUsername(username string) error
	// ValidateUsernameFormat checks if the username is in the correct format.
	ValidateUsernameFormat(username string) bool
	// GetUsernameVariants returns a list of possible username variants for the given username.
	GetUsernameVariants(username string) []string
	// GetOriginalUsername returns the original username from the given username.
	GetOriginalUsername(username string) string
}

var globalUsernamePolicy UsernamePolicy = NewDefaultUsernamePolicy()

// SetUsernamePolicy sets the global username policy.
func SetUsernamePolicy(policy UsernamePolicy) {
	globalUsernamePolicy = policy
}

// GetUsernamePolicy returns the global username policy.
func GetUsernamePolicy() UsernamePolicy {
	return globalUsernamePolicy
}

type defaultUsernamePolicy struct{}

// NewDefaultUsernamePolicy creates a new default username policy.
func NewDefaultUsernamePolicy() UsernamePolicy {
	return &defaultUsernamePolicy{}
}

func (p *defaultUsernamePolicy) ValidateUsername(username string) error {
	return nil
}

func (p *defaultUsernamePolicy) ValidateUsernameFormat(username string) bool {
	return true
}

func (p *defaultUsernamePolicy) GetUsernameVariants(username string) []string {
	return nil
}

func (p *defaultUsernamePolicy) GetOriginalUsername(username string) string {
	return ""
}

type prefixPolicy struct {
	userPrefix string
}

// NewPrefixPolicy creates a new policy that requires the username to have a specific prefix.
func NewPrefixPolicy(userPrefix string) UsernamePolicy {
	return &prefixPolicy{userPrefix: userPrefix}
}

func (p prefixPolicy) ValidateUsernameFormat(username string) bool {
	splits := strings.Split(username, ".")
	return len(splits) == 2
}

func (p prefixPolicy) ValidateUsername(username string) error {
	if p.userPrefix != "" && !strings.HasPrefix(username, p.userPrefix+".") {
		return errmsg.WithUserPrefixErrTag(exeerrors.ErrUserNameNeedPrefix.GenWithStackByArgs(p.userPrefix, p.userPrefix, username))
	}
	return nil
}

func (p prefixPolicy) GetUsernameVariants(username string) []string {
	if p.userPrefix != "" {
		return []string{p.userPrefix + "." + username}
	}
	return nil
}

func (p prefixPolicy) GetOriginalUsername(username string) string {
	if p.userPrefix != "" && strings.HasPrefix(username, p.userPrefix+".") {
		return username[len(p.userPrefix)+1:]
	}
	return ""
}
