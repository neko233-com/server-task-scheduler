// Package scheduler evaluates cron schedules and dispatches due jobs to
// workers that advertise the required task key. It mirrors xxl-job's model:
// the center owns the schedule; executors own the task implementations and
// self-register the tasks they can run.
package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/neko233-com/server-task-scheduler/internal/dispatch"
	"github.com/neko233-com/server-task-scheduler/internal/model"
	"github.com/neko233-com/server-task-scheduler/internal/registry"
	"github.com/neko233-com/server-task-scheduler/internal/store"
	"github.com/neko233-com/server-task-scheduler/internal/util"
	cron "github.com/robfig/cron/v3"
)

// maxCatchup bounds how far back a missed fire may be replayed after downtime.
const maxCatchup = 10 * time.Minute

// Scheduler drives schedule evaluation on a ticker.
type Scheduler struct {
	registry  *registry.Registry
	store     *store.Store
	dispatch  *dispatch.Dispatcher
	interval  time.Duration
	badCron   map[string]bool
	badCronMu sync.Mutex
}

// New constructs a Scheduler.
func New(reg *registry.Registry, st *store.Store, d *dispatch.Dispatcher) *Scheduler {
	return &Scheduler{
		registry: reg,
		store:    st,
		dispatch: d,
		interval: time.Second,
		badCron:  make(map[string]bool),
	}
}

// Start launches the evaluation loop. It returns immediately; use ctx to stop.
func (s *Scheduler) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.tick(ctx)
			}
		}
	}()
}

func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now()
	for _, sc := range s.store.List() {
		if !sc.Enabled {
			continue
		}
		sched, err := cron.ParseStandard(sc.Cron)
		if err != nil {
			s.warnBadCron(sc)
			continue
		}
		after := sc.CreatedAt
		if sc.LastRunAt != nil {
			after = *sc.LastRunAt
		}
		next := sched.Next(after)
		if !next.Before(now) {
			continue // not due yet
		}
		if now.Sub(next) > maxCatchup {
			// Too far in the past (e.g. server was down); skip without executing.
			s.store.MarkRun(sc.ID, now)
			continue
		}
		s.store.MarkRun(sc.ID, now)
		s.fire(ctx, sc)
	}
}

func (s *Scheduler) warnBadCron(sc *model.Schedule) {
	s.badCronMu.Lock()
	defer s.badCronMu.Unlock()
	if s.badCron[sc.Cron] {
		return
	}
	s.badCron[sc.Cron] = true
	log.Printf("[scheduler] invalid cron %q for schedule %s (%s); skipping", sc.Cron, sc.ID, sc.Name)
}

// dispatchTimeout computes the wall-clock timeout for a single dispatch call,
// matching dispatch.Dispatcher's slack handling.
func dispatchTimeout(workerTimeoutSeconds int) time.Duration {
	t := time.Duration(workerTimeoutSeconds) * time.Second
	if t <= 0 {
		t = 60 * time.Second
	}
	return t + 5*time.Second
}

// fire dispatches one schedule to a suitable worker, asynchronously.
func (s *Scheduler) fire(ctx context.Context, sc *model.Schedule) {
	worker := s.registry.PickWorker(sc.TaskKey)
	exec := model.Execution{
		ID:         util.NewID(),
		ScheduleID: sc.ID,
		TaskKey:    sc.TaskKey,
		StartedAt:  time.Now(),
	}
	if worker == nil {
		exec.FinishedAt = time.Now()
		exec.Success = false
		exec.Error = "no online worker provides task_key " + sc.TaskKey
		log.Printf("[scheduler] %s: %s", exec.Error, sc.Name)
		s.store.RecordExecution(exec)
		return
	}
	exec.WorkerID = worker.ID
	exec.WorkerAddr = worker.Address

	go func() {
		req := model.ExecuteRequest{
			JobID:          exec.ID,
			TaskKey:        sc.TaskKey,
			Params:         sc.Params,
			Attempt:        1,
			TimeoutSeconds: sc.TimeoutSeconds,
		}
		dctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout(req.TimeoutSeconds))
		defer cancel()
		resp, err := s.dispatch.Execute(dctx, worker, req)
		exec.FinishedAt = time.Now()
		if err != nil {
			exec.Success = false
			exec.Error = err.Error()
			log.Printf("[scheduler] dispatch %s to %s failed: %v", sc.TaskKey, worker.Address, err)
		} else {
			exec.Success = resp.Success
			exec.Output = resp.Output
			exec.Error = resp.Error
		}
		s.store.RecordExecution(exec)
	}()
}

// TriggerNow forces an immediate dispatch of a schedule (manual trigger API).
// It returns the execution record (the run may still be in flight when the
// caller reads it). hasWorker reports whether a worker was available.
func (s *Scheduler) TriggerNow(ctx context.Context, sc *model.Schedule) (model.Execution, bool) {
	exec := model.Execution{
		ID:         util.NewID(),
		ScheduleID: sc.ID,
		TaskKey:    sc.TaskKey,
		StartedAt:  time.Now(),
	}
	worker := s.registry.PickWorker(sc.TaskKey)
	if worker == nil {
		exec.FinishedAt = time.Now()
		exec.Success = false
		exec.Error = "no online worker provides task_key " + sc.TaskKey
		s.store.RecordExecution(exec)
		return exec, false
	}
	exec.WorkerID = worker.ID
	exec.WorkerAddr = worker.Address
	req := model.ExecuteRequest{
		JobID:          exec.ID,
		TaskKey:        sc.TaskKey,
		Params:         sc.Params,
		Attempt:        1,
		TimeoutSeconds: sc.TimeoutSeconds,
	}
	// The dispatch is asynchronous and must outlive the inbound HTTP request,
	// so use an independent background context with its own timeout rather than
	// the (soon-cancelled) request context.
	go func() {
		dctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout(req.TimeoutSeconds))
		defer cancel()
		resp, err := s.dispatch.Execute(dctx, worker, req)
		exec.FinishedAt = time.Now()
		if err != nil {
			exec.Success = false
			exec.Error = err.Error()
		} else {
			exec.Success = resp.Success
			exec.Output = resp.Output
			exec.Error = resp.Error
		}
		s.store.RecordExecution(exec)
	}()
	return exec, true
}
