// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"crypto/x509"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	serverNotAfter  = "Ssl_server_not_after"
	serverNotBefore = "Ssl_server_not_before"
	upTime          = "Uptime"
)

var defaultStatus = map[string]*variable.StatusVal{
	serverNotAfter:  {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	serverNotBefore: {Scope: variable.ScopeGlobal | variable.ScopeSession, Value: ""},
	upTime:          {Scope: variable.ScopeGlobal, Value: 0},
}

// GetScope gets the Status variables scope.
func (*Server) GetScope(_ string) variable.ScopeFlag {
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the server statistics.
func (s *Server) Stats(_ *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any, len(defaultStatus))

	for name, v := range defaultStatus {
		m[name] = v.Value
	}

	tlsConfig := s.GetTLSConfig()
	if tlsConfig != nil && tlsConfig.GetCertificate != nil {
		certs, err := tlsConfig.GetCertificate(nil)
		if err != nil {
			logutil.BgLogger().Error("Failed to get TLS certificates while acquiring server status", zap.Error(err))
		}
		if certs := certs.Certificate; len(certs) > 0 {
			cert, err := x509.ParseCertificate(certs[0])
			if err != nil {
				logutil.BgLogger().Error("Failed to parse TLS certificates to get server status", zap.Error(err))
			} else {
				m[serverNotAfter] = cert.NotAfter.Format("Jan _2 15:04:05 2006 MST")
				m[serverNotBefore] = cert.NotBefore.Format("Jan _2 15:04:05 2006 MST")
			}
		}
	}

	var err error
	info := tikvhandler.ServerInfo{}
	info.ServerInfo, err = infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Error("Failed to get ServerInfo for uptime status", zap.Error(err))
	} else {
		m[upTime] = int64(time.Since(time.Unix(info.ServerInfo.StartTimestamp, 0)).Seconds())
	}

	return m, nil
}
