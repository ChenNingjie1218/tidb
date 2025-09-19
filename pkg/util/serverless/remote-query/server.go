// Copyright 2024 PingCAP, Inc.
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

package remotequery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/tidbworker"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// SessionMaxIdleTime is the max idle time for a session.
	SessionMaxIdleTime = 3 * time.Minute
	// CheckSessionIdleInterval is the interval to check session idle.
	CheckSessionIdleInterval = time.Second
)

// Session is a remote query session.
type Session struct {
	QueryID    string `json:"queryID"`
	Query      string `json:"query"`
	User       string `json:"user"`
	UserHost   string `json:"userHost"`
	DB         string `json:"db"`
	StartTime  time.Time
	LastActive atomic.Int64
	RecordSet  *RecordSet
}

// Server is a remote query server.
type Server struct {
	scalerAddr    string
	advertiseAddr string
	closed        chan struct{}

	sync.RWMutex
	sessions       map[string]*Session
	watcherStarted bool
}

// NewServer creates a new remote query server.
func NewServer() *Server {
	s := &Server{
		sessions:      make(map[string]*Session),
		advertiseAddr: config.GetGlobalConfig().AdvertiseAddress,
		closed:        make(chan struct{}),
	}
	return s
}

func (s *Server) watchIdleSessions() {
	idleTicker := time.NewTicker(CheckSessionIdleInterval)
	defer idleTicker.Stop()
	for {
		select {
		case <-idleTicker.C:
			s.Lock()
			metrics.RemoteQuerySessionGauge.Set(float64(len(s.sessions)))
			for id, session := range s.sessions {
				if time.Since(time.Unix(session.LastActive.Load(), 0)) > SessionMaxIdleTime {
					logutil.BgLogger().Info("close idle session", zap.String("queryID", id))
					session.RecordSet.Close()
					delete(s.sessions, id)
					metrics.RemoteQuerySessionCounter.WithLabelValues("close_idle").Inc()
				}
			}
			s.Unlock()
		case <-s.closed:
			logutil.BgLogger().Info("remote query server is closed")
			metrics.RemoteQuerySessionGauge.Set(0)
			return
		}
	}
}

// Close closes the server.
func (s *Server) Close() {
	close(s.closed)
}

// RegisterSession registers a new session.
func (s *Server) RegisterSession(ctx context.Context, query, currentDB string, user *auth.UserIdentity, chunkInitCap, chunkMaxSize int) (sqlexec.RecordSet, error) {
	s.Lock()
	defer s.Unlock()
	session := &Session{
		QueryID:   uuid.NewString(),
		Query:     query,
		DB:        currentDB,
		User:      user.Username,
		UserHost:  user.Hostname,
		StartTime: time.Now(),
		RecordSet: NewRecordSet(chunkInitCap, chunkMaxSize),
	}
	session.LastActive.Store(time.Now().Unix())

	logutil.BgLogger().Info("register session", zap.String("queryID", session.QueryID), zap.String("user", user.String()), zap.String("db", currentDB))
	url := fmt.Sprintf("%s:10080/remote-query/%s", s.advertiseAddr, session.QueryID)
	err := tidbworker.GlobalTiDBWorkerManager.RegisterRemoteQuery(ctx, session.QueryID, url)
	if err != nil {
		logutil.BgLogger().Error("register remote query failed", zap.Error(err))
		metrics.RemoteQuerySessionCounter.WithLabelValues("register_failed").Inc()
		return nil, err
	}

	metrics.RemoteQuerySessionCounter.WithLabelValues("register").Inc()
	s.sessions[session.QueryID] = session

	if !s.watcherStarted { // lazy start idle session watcher
		go s.watchIdleSessions()
		s.watcherStarted = true
	}

	return session.RecordSet, nil
}

// HandleGetQuery handles the get query request.
func (s *Server) HandleGetQuery(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	queryID := vars["query-id"]
	s.RLock()
	session, ok := s.sessions[queryID]
	s.RUnlock()
	if !ok {
		http.NotFound(w, r)
		metrics.RemoteQueryServerCounter.WithLabelValues("get_query", "not_found").Inc()
		return
	}
	metrics.RemoteQueryServerCounter.WithLabelValues("get_query", "ok").Inc()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(session)
}

// HandlePostQuery handles the post query request.
func (s *Server) HandlePostQuery(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	queryID := vars["query-id"]
	s.RLock()
	session, ok := s.sessions[queryID]
	s.RUnlock()
	if !ok {
		http.NotFound(w, r)
		metrics.RemoteQueryServerCounter.WithLabelValues("post_chunk", "not_found").Inc()
		return
	}
	session.LastActive.Store(time.Now().Unix())
	data, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		logutil.BgLogger().Error("read request body failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		metrics.RemoteQueryServerCounter.WithLabelValues("post_chunk", "failed").Inc()
		return
	}
	session.RecordSet.RecvData(data)
	w.WriteHeader(http.StatusOK)
	metrics.RemoteQueryServerCounter.WithLabelValues("post_chunk", "ok").Inc()
}

// HandleError handles the error request.
func (s *Server) HandleError(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	queryID := vars["query-id"]
	s.RLock()
	session, ok := s.sessions[queryID]
	s.RUnlock()
	if !ok {
		http.NotFound(w, r)
		metrics.RemoteQueryServerCounter.WithLabelValues("error", "not_found").Inc()
		return
	}
	session.LastActive.Store(time.Now().Unix())
	data, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		logutil.BgLogger().Error("read request body failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		metrics.RemoteQueryServerCounter.WithLabelValues("recv_error", "failed").Inc()
		return
	}
	session.RecordSet.RecvError(fmt.Errorf("remote executor error: %s", data))
	w.WriteHeader(http.StatusOK)
	metrics.RemoteQueryServerCounter.WithLabelValues("recv_error", "ok").Inc()
}

// HandlePing handles the ping request.
func (s *Server) HandlePing(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	queryID := vars["query-id"]
	s.RLock()
	session, ok := s.sessions[queryID]
	s.RUnlock()
	if !ok {
		http.NotFound(w, r)
		metrics.RemoteQueryServerCounter.WithLabelValues("ping", "not_found").Inc()
		return
	}
	session.LastActive.Store(time.Now().Unix())
	w.WriteHeader(http.StatusOK)
	metrics.RemoteQueryServerCounter.WithLabelValues("ping", "ok").Inc()
}
