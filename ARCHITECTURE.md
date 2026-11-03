# Architecture — server-task-scheduler

A distributed, language-agnostic task scheduler inspired by [xxl-job](https://github.com/xuxueli/xxl-job).
The center owns **schedules**; executors (workers) own **task implementations** and
**self-register** the tasks they can run. The center dispatches a due job to any
online worker that advertises the required task key.

## Why this shape

- **Auto-registration.** Workers announce themselves and their task catalog on
  startup and stay alive via heartbeat. No manual worker configuration in the
  center.
- **Polyglot by design.** The control plane is plain HTTP/JSON, so a worker can
  be written in any language. Officially maintained SDKs: Go, Rust, C#, TypeScript.
- **Copy-in friendly SDKs.** The SDKs are tiny and meant to be vendored into your
  project (copy the folder / use as a git submodule). They are intentionally not
  published to central package registries, so there is no publish step to block adoption.

## Components

```
            ┌─────────────────────────────┐
            │   server-task-scheduler     │   :6432 (default)
            │  ─────────────────────────  │
            │  registry (workers online)  │
            │  store    (schedules.json)  │
            │  scheduler (cron -> dispatch)│
            │  api       (/api/v1/...)    │
            └──────────────┬──────────────┘
        register/heartbeat │  │ dispatch /execute
                           │  ▼
                  ┌──────────────────┐
                  │  Worker (SDK)    │   :8081 (example)
                  │  task catalog    │
                  │  /execute handler│
                  └──────────────────┘
```

## Wire contract (HTTP/JSON)

Base URL of the center, e.g. `http://localhost:6432`. Worker callback base URL,
e.g. `http://localhost:8081`.

### Worker → Center

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/workers/register` | Register on startup. Body: `{name, address, app, tasks[]}`. Returns `{worker_id, ttl_seconds}`. |
| POST | `/api/v1/workers/heartbeat` | Stay alive. Body: `{worker_id, tasks[]}`. Refresh task list optionally. |
| POST | `/api/v1/workers/deregister` | Remove on shutdown. Body: `{worker_id}`. |
| GET  | `/api/v1/workers` | List online workers. |
| GET  | `/api/v1/tasks` | Task catalog: task_key → worker ids. |

### Center → Worker

| Method | Path | Purpose |
|--------|------|---------|
| POST | `{worker.address}/execute` | Run a task. Body: `{job_id, task_key, params, attempt, timeout_seconds}`. Returns `{success, output, error}`. |

### Admin → Center

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/schedules` | Create schedule. Body: `{name, task_key, cron, params, enabled, timeout_seconds}`. |
| GET  | `/api/v1/schedules` | List schedules. |
| GET  | `/api/v1/schedules/{id}` | Get one. |
| PUT  | `/api/v1/schedules/{id}` | Update. |
| DELETE | `/api/v1/schedules/{id}` | Delete. |
| POST | `/api/v1/schedules/{id}/trigger` | Fire immediately (manual). |
| GET  | `/api/v1/executions?limit=50` | Recent execution records. |
| GET  | `/healthz` | Liveness. |

## Scheduling semantics

- Cron uses the **standard 5-field** format (`robfig/cron`): `minute hour day month weekday`.
- On each tick the scheduler computes the next fire time for every enabled
  schedule. When due, it picks an online worker providing `task_key`
  (round-robin) and calls its `/execute`.
- If no online worker provides the task key, the fire is recorded as failed with
  `no online worker provides task_key <key>` and retried on the next cycle.
- Missed fires older than 10 minutes (e.g. after downtime) are skipped, not replayed.
- Schedules persist to `<DATA_DIR>/scheduler.json`. Workers are ephemeral.

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `PORT` | `6432` | Listen port. |
| `DATA_DIR` | `./data` | Directory for `scheduler.json`. |
| `WORKER_TTL` | `30s` | Heartbeat TTL; workers expiring beyond it are purged. |

## SDK repositories (submodules)

- `sdk/go` → [task-scheduler-lib-go](https://github.com/neko233-com/task-scheduler-lib-go)
- `sdk/rust` → [task-scheduler-lib-rust](https://github.com/neko233-com/task-scheduler-lib-rust)
- `sdk/csharp` → [task-scheduler-lib-csharp](https://github.com/neko233-com/task-scheduler-lib-csharp)
- `sdk/typescript` → [task-scheduler-lib-typescript](https://github.com/neko233-com/task-scheduler-lib-typescript)
