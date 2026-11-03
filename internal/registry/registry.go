// Package registry maintains the live set of worker nodes.
//
// Workers are ephemeral: they appear via Register, stay alive via Heartbeat,
// and are purged when no heartbeat is seen within the TTL. Task-to-worker
// routing is derived from this registry.
package registry

import (
	"sync"
	"time"

	"github.com/neko233-com/server-task-scheduler/internal/model"
	"github.com/neko233-com/server-task-scheduler/internal/util"
)

// Registry is a concurrency-safe in-memory worker registry.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]*model.Worker
	ttl     time.Duration
	// rr is a per-task-key round-robin cursor for dispatch selection.
	rr map[string]int
}

// New creates a registry with the given heartbeat TTL.
func New(ttl time.Duration) *Registry {
	return &Registry{
		workers: make(map[string]*model.Worker),
		ttl:     ttl,
		rr:      make(map[string]int),
	}
}

// TTL returns the configured heartbeat TTL in seconds.
func (r *Registry) TTL() int {
	return int(r.ttl.Seconds())
}

// Register adds or replaces a worker and returns it with a fresh timestamp.
func (r *Registry) Register(req model.RegisterRequest) *model.Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	w := &model.Worker{
		ID:           util.NewID(),
		Name:         req.Name,
		App:          req.App,
		Address:      req.Address,
		Tasks:        req.Tasks,
		LastSeen:     now,
		RegisteredAt: now,
	}
	r.workers[w.ID] = w
	return w
}

// Heartbeat refreshes a worker's last-seen time and (optionally) its task list.
// It reports whether the worker was known.
func (r *Registry) Heartbeat(req model.HeartbeatRequest) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[req.WorkerID]
	if !ok {
		return false
	}
	w.LastSeen = time.Now()
	if len(req.Tasks) > 0 {
		w.Tasks = req.Tasks
	}
	return true
}

// Deregister removes a worker immediately.
func (r *Registry) Deregister(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, workerID)
}

// List returns all workers that have a recent heartbeat (online).
func (r *Registry) List() []*model.Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now()
	out := make([]*model.Worker, 0, len(r.workers))
	for _, w := range r.workers {
		if now.Sub(w.LastSeen) <= r.ttl {
			out = append(out, w)
		}
	}
	return out
}

// WorkersForTask returns the online workers that advertise the given task key.
func (r *Registry) WorkersForTask(taskKey string) []*model.Worker {
	online := r.List()
	out := make([]*model.Worker, 0)
	for _, w := range online {
		for _, t := range w.Tasks {
			if t == taskKey {
				out = append(out, w)
				break
			}
		}
	}
	return out
}

// PickWorker returns a worker for taskKey using round-robin selection across
// the online providers. Returns nil if none are available.
func (r *Registry) PickWorker(taskKey string) *model.Worker {
	candidates := r.WorkersForTask(taskKey)
	if len(candidates) == 0 {
		return nil
	}
	r.mu.Lock()
	cursor := r.rr[taskKey] % len(candidates)
	r.rr[taskKey] = cursor + 1
	r.mu.Unlock()
	return candidates[cursor]
}

// PurgeExpired removes workers whose last heartbeat is older than the TTL.
// Returns the number of workers removed.
func (r *Registry) PurgeExpired() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	removed := 0
	for id, w := range r.workers {
		if now.Sub(w.LastSeen) > r.ttl {
			delete(r.workers, id)
			removed++
		}
	}
	return removed
}

// Catalog builds a task-key -> worker-ids map from the online workers.
func (r *Registry) Catalog() []model.TaskCatalogEntry {
	online := r.List()
	idx := make(map[string]*model.TaskCatalogEntry)
	order := make([]string, 0)
	for _, w := range online {
		for _, t := range w.Tasks {
			e, ok := idx[t]
			if !ok {
				e = &model.TaskCatalogEntry{TaskKey: t}
				idx[t] = e
				order = append(order, t)
			}
			e.Workers = append(e.Workers, w.ID)
		}
	}
	out := make([]model.TaskCatalogEntry, 0, len(order))
	for _, t := range order {
		out = append(out, *idx[t])
	}
	return out
}
