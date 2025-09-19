// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// HealthHandler is the http handler for health check.
type HealthHandler struct {
	dom *domain.Domain
	sm  util.SessionManager
}

// NewHealthHandler creates a new health handler.
func NewHealthHandler(dom *domain.Domain, sm util.SessionManager) *HealthHandler {
	return &HealthHandler{dom: dom, sm: sm}
}

type healthResponse struct {
	Status string `json:"status"`
	Token  string `json:"token"`
	Error  string `json:"error,omitempty"`
}

// ServeHTTP implements the http.Handler interface.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()

	var err error
	response := &healthResponse{
		Status: "up",
		Token:  config.GetGlobalKeyspaceName(),
	}
	defer func() {
		if err != nil && !terror.ErrorEqual(err, storeerr.ErrResourceGroupThrottled) { // ignore throttled error
			response.Status = "down"
			response.Error = err.Error()
		}
		json.NewEncoder(w).Encode(response)
	}()

	se, err := session.CreateSessionWithDomain(h.dom.Store(), h.dom)
	if se != nil {
		defer se.Close()
	}
	if err != nil {
		return
	}
	se.SetSessionManager(h.sm)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	rs, err := se.ExecuteInternal(ctx, "SELECT variable_name FROM mysql.tidb LIMIT 1")
	if err != nil {
		return
	}
	defer rs.Close()
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return
	}
	if len(rows) != 1 {
		err = errors.New("timeout to read mysql.tidb")
	}
}
