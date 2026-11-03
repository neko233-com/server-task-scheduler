// Package api exposes the HTTP/JSON control plane of the dispatch center.
// All endpoints are versioned under /api/v1. See ARCHITECTURE.md for the
// full request/response contract shared with the language SDKs.
package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/neko233-com/server-task-scheduler/internal/model"
	"github.com/neko233-com/server-task-scheduler/internal/registry"
	"github.com/neko233-com/server-task-scheduler/internal/scheduler"
	"github.com/neko233-com/server-task-scheduler/internal/store"
)

// Server wires the registry/store/scheduler into HTTP handlers.
type Server struct {
	reg  *registry.Registry
	st   *store.Store
	sch  *scheduler.Scheduler
}

// New builds the API server.
func New(reg *registry.Registry, st *store.Store, sch *scheduler.Scheduler) *Server {
	return &Server{reg: reg, st: st, sch: sch}
}

// Handler returns the configured http.Handler (Go 1.22+ enhanced mux).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /api/v1/workers/register", s.handleRegister)
	mux.HandleFunc("POST /api/v1/workers/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("POST /api/v1/workers/deregister", s.handleDeregister)
	mux.HandleFunc("GET /api/v1/workers", s.handleListWorkers)
	mux.HandleFunc("GET /api/v1/tasks", s.handleCatalog)
	mux.HandleFunc("POST /api/v1/schedules", s.handleCreateSchedule)
	mux.HandleFunc("GET /api/v1/schedules", s.handleListSchedules)
	mux.HandleFunc("GET /api/v1/schedules/{id}", s.handleGetSchedule)
	mux.HandleFunc("PUT /api/v1/schedules/{id}", s.handleUpdateSchedule)
	mux.HandleFunc("DELETE /api/v1/schedules/{id}", s.handleDeleteSchedule)
	mux.HandleFunc("POST /api/v1/schedules/{id}/trigger", s.handleTrigger)
	mux.HandleFunc("GET /api/v1/executions", s.handleExecutions)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req model.RegisterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Address == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{"address is required"})
		return
	}
	wk := s.reg.Register(req)
	log.Printf("[api] worker registered id=%s name=%s tasks=%v", wk.ID, wk.Name, wk.Tasks)
	writeJSON(w, http.StatusOK, model.RegisterResponse{WorkerID: wk.ID, TTLSeconds: s.reg.TTL()})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req model.HeartbeatRequest
	if !decode(w, r, &req) {
		return
	}
	ok := s.reg.Heartbeat(req)
	if !ok {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{"unknown worker_id"})
		return
	}
	writeJSON(w, http.StatusOK, model.GenericAck{OK: true, TTLSeconds: s.reg.TTL()})
}

func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	s.reg.Deregister(req.WorkerID)
	writeJSON(w, http.StatusOK, model.GenericAck{OK: true})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"workers": s.reg.List()})
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"tasks": s.reg.Catalog()})
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	var req model.ScheduleRequest
	if !decode(w, r, &req) {
		return
	}
	if req.TaskKey == "" || req.Cron == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{"task_key and cron are required"})
		return
	}
	sc := s.st.Create(req)
	writeJSON(w, http.StatusCreated, sc)
}

func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"schedules": s.st.List()})
}

func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sc, ok := s.st.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{"schedule not found"})
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

func (s *Server) handleUpdateSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req model.ScheduleRequest
	if !decode(w, r, &req) {
		return
	}
	sc, ok := s.st.Update(id, req)
	if !ok {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{"schedule not found"})
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.st.Delete(id) {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{"schedule not found"})
		return
	}
	writeJSON(w, http.StatusOK, model.GenericAck{OK: true})
}

func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sc, ok := s.st.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, model.ErrorResponse{"schedule not found"})
		return
	}
	exec, hasWorker := s.sch.TriggerNow(r.Context(), sc)
	if !hasWorker {
		writeJSON(w, http.StatusConflict, exec)
		return
	}
	writeJSON(w, http.StatusAccepted, exec)
}

func (s *Server) handleExecutions(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"executions": s.st.ListExecutions(limit)})
}

func decode(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{"invalid JSON: " + err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
