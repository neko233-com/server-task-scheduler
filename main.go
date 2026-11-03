// Command server-task-scheduler is the dispatch center for the
// task-scheduler ecosystem. It auto-discovers workers (which self-register and
// advertise the tasks they can run), stores cron schedules, and dispatches due
// jobs to a suitable online worker.
//
// Default port is 6432 (override with PORT). See ARCHITECTURE.md.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neko233-com/server-task-scheduler/internal/api"
	"github.com/neko233-com/server-task-scheduler/internal/dispatch"
	"github.com/neko233-com/server-task-scheduler/internal/registry"
	"github.com/neko233-com/server-task-scheduler/internal/scheduler"
	"github.com/neko233-com/server-task-scheduler/internal/store"
)

func main() {
	port := envOr("PORT", "6432")
	dataDir := envOr("DATA_DIR", "./data")
	ttl := envDuration("WORKER_TTL", 30*time.Second)

	reg := registry.New(ttl)
	st, err := store.New(dataDir)
	if err != nil {
		log.Fatalf("store init failed: %v", err)
	}
	disp := dispatch.New()
	sch := scheduler.New(reg, st, disp)
	sch.Start(context.Background())

	// Purge expired workers periodically.
	go func() {
		t := time.NewTicker(ttl)
		defer t.Stop()
		for range t.C {
			if n := reg.PurgeExpired(); n > 0 {
				log.Printf("[registry] purged %d expired worker(s)", n)
			}
		}
	}()

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: api.New(reg, st, sch).Handler(),
	}

	go func() {
		log.Printf("task-scheduler dispatch center listening on :%s (data=%s, worker_ttl=%s)", port, dataDir, ttl)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	// Graceful shutdown.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
