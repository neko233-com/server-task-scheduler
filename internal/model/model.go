// Package model defines the shared data structures exchanged between the
// dispatch center (server-task-scheduler) and the language SDK workers.
//
// The wire format is JSON over HTTP. See ARCHITECTURE.md for the full contract.
package model

import "time"

// Worker is a registered executor node. It is kept in the in-memory registry
// and expires when its heartbeat TTL lapses.
type Worker struct {
	ID           string    `json:"worker_id"`
	Name         string    `json:"name"`
	App          string    `json:"app"`
	Address      string    `json:"address"` // callback base URL, e.g. http://10.0.0.5:8081
	Tasks        []string  `json:"tasks"`  // task keys this worker can execute
	LastSeen     time.Time `json:"last_seen"`
	RegisteredAt time.Time `json:"registered_at"`
}

// RegisterRequest is sent by a worker on startup.
type RegisterRequest struct {
	Name    string   `json:"name"`
	Address string   `json:"address"`
	App     string   `json:"app"`
	Tasks   []string `json:"tasks"`
}

// RegisterResponse returns the assigned worker id and the heartbeat TTL the
// server expects.
type RegisterResponse struct {
	WorkerID  string `json:"worker_id"`
	TTLSeconds int   `json:"ttl_seconds"`
}

// HeartbeatRequest keeps a worker alive and optionally refreshes its task list.
type HeartbeatRequest struct {
	WorkerID string   `json:"worker_id"`
	Tasks    []string `json:"tasks"`
}

// GenericAck is returned by heartbeat/deregister.
type GenericAck struct {
	OK        bool `json:"ok"`
	TTLSeconds int `json:"ttl_seconds"`
}

// Schedule is a cron-driven job definition. When it fires, the scheduler picks
// an online worker that provides TaskKey and dispatches an execution.
type Schedule struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	TaskKey        string                 `json:"task_key"`
	Cron           string                 `json:"cron"` // standard 5-field cron
	Params         map[string]interface{} `json:"params"`
	Enabled        bool                   `json:"enabled"`
	TimeoutSeconds int                    `json:"timeout_seconds"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
	LastRunAt      *time.Time             `json:"last_run_at,omitempty"`
}

// ScheduleRequest is the create/update payload for a Schedule.
type ScheduleRequest struct {
	Name           string                 `json:"name"`
	TaskKey        string                 `json:"task_key"`
	Cron           string                 `json:"cron"`
	Params         map[string]interface{} `json:"params"`
	Enabled        bool                   `json:"enabled"`
	TimeoutSeconds int                    `json:"timeout_seconds"`
}

// ExecuteRequest is what the server sends to a worker's /execute endpoint.
type ExecuteRequest struct {
	JobID          string                 `json:"job_id"`
	TaskKey        string                 `json:"task_key"`
	Params         map[string]interface{} `json:"params"`
	Attempt        int                    `json:"attempt"`
	TimeoutSeconds int                    `json:"timeout_seconds"`
}

// ExecuteResponse is returned by a worker after running a task.
type ExecuteResponse struct {
	Success bool   `json:"success"`
	Output  string `json:"output"`
	Error   string `json:"error"`
}

// Execution is a persisted record of one dispatch attempt.
type Execution struct {
	ID           string    `json:"id"`
	ScheduleID   string    `json:"schedule_id"`
	TaskKey      string    `json:"task_key"`
	WorkerID     string    `json:"worker_id"`
	WorkerAddr   string    `json:"worker_address"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Success      bool      `json:"success"`
	Output       string    `json:"output"`
	Error        string    `json:"error"`
}

// TaskCatalogEntry describes which workers currently provide a task key.
type TaskCatalogEntry struct {
	TaskKey string   `json:"task_key"`
	Workers []string `json:"workers"` // worker ids
}

// ErrorResponse is the standard error envelope.
type ErrorResponse struct {
	Error string `json:"error"`
}
