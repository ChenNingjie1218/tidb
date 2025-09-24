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

package config

const (
	// LogFormatText is stardard log format surrounded with []
	LogFormatText = "TEXT"
	// LogFormatJSON is json format
	LogFormatJSON = "JSON"
)

// AuditLog is the config for serverless audit log
type AuditLog struct {
	// Enable indicates whether audit log will be recorded
	Enable bool `toml:"enable" json:"enable"`
	// Path is the audit log output path
	Path string `toml:"path" json:"path"`
	// Format is the output format of audit log, both text and json are supported
	Format string `toml:"format" json:"format"`
	// MaxFilesize is the maximum file size before the log file be rotated, unit is MB
	MaxFilesize int64 `toml:"max-filesize" json:"max-filesize"`
	// MaxLifetime is the maximum time before the log file be rotated, unit is second
	MaxLifetime int64 `toml:"max-lifetime" json:"max-lifetime"`
	// ReservedBackups is the number of rotated log files to reserve.
	ReservedBackups int `toml:"reserved-backups" json:"reserved-backups"`
	// ReservedDays is the day to reseve rotated log files.
	ReservedDays int `toml:"reserved-days" json:"reserved-days"`
	// Redacted indicates whether audit log redaction is enabled. If it set to true, user data will be replaced with `?`
	Redacted bool `toml:"redacted" json:"redacted"`
	// EncryptKey is used to encrypt sensitive informations in audit log. This key should be 32 bytes, we use AES-256 for encryption
	EncryptKey string `toml:"encrypt-key" json:"encrypt-key"`
}

func defaultAuditLog() AuditLog {
	return AuditLog{
		Enable:          false,
		Path:            "tidb-audit.log",
		Format:          LogFormatText,
		MaxFilesize:     1,
		MaxLifetime:     24 * 60 * 60,
		ReservedBackups: 1,
		ReservedDays:    0,
		Redacted:        true,
		EncryptKey:      "",
	}
}

// BootstrapControl contains serverless bootstrap configuration options.
type BootstrapControl struct {
	SkipServerlessVariables bool `toml:"skip-serverless-variables" json:"skip-serverless-variables"`
	SkipRootPriv            bool `toml:"skip-root-priv" json:"skip-root-priv"`
	SkipCloudAdminPriv      bool `toml:"skip-cloud-admin-priv" json:"skip-cloud-admin-priv"`
	SkipRoleAdminPriv       bool `toml:"skip-role-admin-priv" json:"skip-role-admin-priv"`
	SkipPushdownBlacklist   bool `toml:"skip-pushdown-blacklist" json:"skip-pushdown-blacklist"`
}

// defaultBootstrapControl creates a new BootstrapControl.
func defaultBootstrapControl() BootstrapControl {
	return BootstrapControl{
		SkipServerlessVariables: false,
		SkipRootPriv:            false,
		SkipCloudAdminPriv:      false,
		SkipRoleAdminPriv:       false,
		SkipPushdownBlacklist:   false,
	}
}

// DefaultResourceGroup is the default resource group name for all txns and snapshots.
var DefaultResourceGroup string
