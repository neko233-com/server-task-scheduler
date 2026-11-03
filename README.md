# server-task-scheduler

A distributed, language-agnostic **task scheduler & dispatch center**, inspired by
[xxl-job](https://github.com/xuxueli/xxl-job). The center owns **schedules**;
executors (**workers**) own **task implementations** and **self-register** the
tasks they can run. When a cron schedule fires, the center dispatches it to any
online worker that advertises the required task key.

- **Auto-registration** — workers register + heartbeat; no manual worker config.
- **Polyglot** — control plane is plain HTTP/JSON. SDKs for Go / Rust / C# / TypeScript.
- **Copy-in friendly SDKs** — vendored into your project (or used as a submodule).
  No package-registry publish step required.

> Default port: **6432** (override with `PORT`). Full contract in [ARCHITECTURE.md](./ARCHITECTURE.md).

## Run

Requires Go 1.22+.

```bash
go run .                       # listens on :6432, persists to ./data/scheduler.json
PORT=8080 go run .             # custom port
WORKER_TTL=30s DATA_DIR=./data go run .
```

## Create a schedule

```bash
curl -X POST http://localhost:6432/api/v1/schedules \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","task_key":"hello","cron":"*/30 * * * *","params":{"name":"world"},"enabled":true,"timeout_seconds":30}'
```

## Watch it run (end-to-end)

The center only fires jobs when an online worker provides the `task_key`. Run a
worker from one of the SDKs (they live under `sdk/` as public submodules):

```bash
# pick any SDK, e.g. Go:
cd sdk/go/example
go run .
```

The worker auto-registers `hello`, the schedule fires every 30s, and the center
dispatches to it. Inspect results:

```bash
curl http://localhost:6432/api/v1/workers
curl http://localhost:6432/api/v1/tasks
curl http://localhost:6432/api/v1/executions?limit=10
```

## Repository layout

```
server-task-scheduler/
├── main.go                 # bootstrap (port 6432, graceful shutdown)
├── internal/
│   ├── model/              # shared wire structs
│   ├── registry/           # online worker registry + task routing
│   ├── store/              # schedules persistence (scheduler.json) + execution log
│   ├── dispatch/           # server -> worker /execute calls
│   ├── scheduler/          # cron evaluation + dispatch loop
│   └── api/                # HTTP/JSON control plane
├── sdk/                    # public submodules (see ARCHITECTURE.md)
│   ├── go/  rust/  csharp/  typescript/
├── ARCHITECTURE.md         # protocol contract
└── README.md
```

## SDKs

| Language | Repo |
|----------|------|
| Go | [task-scheduler-lib-go](https://github.com/neko233-com/task-scheduler-lib-go) |
| Rust | [task-scheduler-lib-rust](https://github.com/neko233-com/task-scheduler-lib-rust) |
| C# | [task-scheduler-lib-csharp](https://github.com/neko233-com/task-scheduler-lib-csharp) |
| TypeScript | [task-scheduler-lib-typescript](https://github.com/neko233-com/task-scheduler-lib-typescript) |
