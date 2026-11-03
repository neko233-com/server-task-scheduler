// Package store persists schedules to disk and keeps a bounded in-memory log of
// executions. Workers are intentionally NOT persisted: they are discovered at
// runtime via registration/heartbeat (xxl-job style auto-registration).
package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/neko233-com/server-task-scheduler/internal/model"
	"github.com/neko233-com/server-task-scheduler/internal/util"
)

const maxExecutions = 500

// Store is a concurrency-safe schedule + execution store.
type Store struct {
	mu         sync.RWMutex
	schedules  map[string]*model.Schedule
	executions []model.Execution
	dataFile   string
}

// New opens (or creates) the store at dataDir/scheduler.json.
func New(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		schedules:  make(map[string]*model.Schedule),
		executions: make([]model.Execution, 0),
		dataFile:   filepath.Join(dataDir, "scheduler.json"),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.dataFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var payload struct {
		Schedules []*model.Schedule `json:"schedules"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	for _, sc := range payload.Schedules {
		s.schedules[sc.ID] = sc
	}
	return nil
}

// Save writes the current schedules to disk. Callers MUST hold s.mu (write lock):
// Create/Update/Delete call Save while holding the lock.
func (s *Store) Save() error {
	payload := struct {
		Schedules []*model.Schedule `json:"schedules"`
	}{
		Schedules: make([]*model.Schedule, 0, len(s.schedules)),
	}
	for _, sc := range s.schedules {
		payload.Schedules = append(payload.Schedules, sc)
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.dataFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.dataFile)
}

// Create adds a schedule.
func (s *Store) Create(req model.ScheduleRequest) *model.Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	sc := &model.Schedule{
		ID:             util.NewID(),
		Name:           req.Name,
		TaskKey:        req.TaskKey,
		Cron:           req.Cron,
		Params:         req.Params,
		Enabled:        req.Enabled,
		TimeoutSeconds: req.TimeoutSeconds,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if sc.TimeoutSeconds <= 0 {
		sc.TimeoutSeconds = 60
	}
	if sc.Params == nil {
		sc.Params = map[string]interface{}{}
	}
	s.schedules[sc.ID] = sc
	_ = s.Save()
	return sc
}

// Update replaces an existing schedule. Returns false if not found.
func (s *Store) Update(id string, req model.ScheduleRequest) (*model.Schedule, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.schedules[id]
	if !ok {
		return nil, false
	}
	sc.Name = req.Name
	sc.TaskKey = req.TaskKey
	sc.Cron = req.Cron
	sc.Params = req.Params
	sc.Enabled = req.Enabled
	sc.TimeoutSeconds = req.TimeoutSeconds
	if sc.TimeoutSeconds <= 0 {
		sc.TimeoutSeconds = 60
	}
	if sc.Params == nil {
		sc.Params = map[string]interface{}{}
	}
	sc.UpdatedAt = time.Now()
	_ = s.Save()
	return sc, true
}

// Delete removes a schedule. Returns true if it existed.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schedules[id]; !ok {
		return false
	}
	delete(s.schedules, id)
	_ = s.Save()
	return true
}

// Get returns a schedule by id.
func (s *Store) Get(id string) (*model.Schedule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sc, ok := s.schedules[id]
	return sc, ok
}

// List returns all schedules.
func (s *Store) List() []*model.Schedule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Schedule, 0, len(s.schedules))
	for _, sc := range s.schedules {
		out = append(out, sc)
	}
	return out
}

// MarkRun records the last-run time for a schedule (called by the scheduler).
func (s *Store) MarkRun(id string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sc, ok := s.schedules[id]; ok {
		t := at
		sc.LastRunAt = &t
		sc.UpdatedAt = at
	}
}

// RecordExecution appends an execution record (bounded ring buffer).
func (s *Store) RecordExecution(e model.Execution) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executions = append(s.executions, e)
	if len(s.executions) > maxExecutions {
		s.executions = s.executions[len(s.executions)-maxExecutions:]
	}
}

// ListExecutions returns the most recent executions (newest first), capped.
func (s *Store) ListExecutions(limit int) []model.Execution {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.executions)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]model.Execution, 0, limit)
	for i := n - 1; i >= n-limit; i-- {
		out = append(out, s.executions[i])
	}
	return out
}
