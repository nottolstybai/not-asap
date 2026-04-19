# not-asap

[![Go Reference](https://pkg.go.dev/badge/github.com/nottolstybai/not-asap.svg)](https://pkg.go.dev/github.com/nottolstybai/not-asap)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://go.dev/)

**not-asap** is a Go library for running background jobs on top of PostgreSQL. No Redis, no RabbitMQ, no separate broker — just the database your service already has.

*Читать на русском — [README.ru.md](README.ru.md).*

## Features

- Enqueue jobs inside an existing business transaction (`EnqueueTx`) — the outbox pattern, built-in.
- Type-safe handlers via generics — your handler receives a fully-parsed payload, not raw bytes.
- Retries with configurable backoff (static or exponential), globally or per job type.
- Cancel a job without further retries by returning `ErrCancel`.
- Batch-insert multiple jobs in a single round-trip.
- Graceful shutdown: in-flight jobs are not lost.
- Optional automatic cleanup of terminal rows (janitor) and Prometheus metrics.

## Requirements

- Go 1.21+
- PostgreSQL 12+
- A `*pgxpool.Pool` from [`pgx/v5`](https://github.com/jackc/pgx) — the library accepts a pool you already own.

## Installation

```bash
go get github.com/nottolstybai/not-asap
```

## Applying migrations

Before starting the queue, apply the migration below using any tool you like.

<details>
<summary>goose</summary>

```sql
-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS notasap_jobs (
    id                UUID        NOT NULL DEFAULT gen_random_uuid(),
    job_type          TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}',
    status            TEXT        NOT NULL DEFAULT 'new',
    run_after         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    error             TEXT,
    last_heartbeat_at TIMESTAMPTZ,
    worker_id         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
    CONSTRAINT notasap_jobs_status_check CHECK (
        status IN ('new','running','done','failed','retrying','exhausted','cancelled')
    )
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
    ON notasap_jobs (status, run_after)
    WHERE status IN ('new', 'retrying');
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
    ON notasap_jobs (status, last_heartbeat_at)
    WHERE status = 'running';
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
    ON notasap_jobs (status, updated_at)
    WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS notasap_jobs;
-- +goose StatementEnd
```

</details>

<details>
<summary>golang-migrate</summary>

`000001_notasap_jobs.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS notasap_jobs (
    id                UUID        NOT NULL DEFAULT gen_random_uuid(),
    job_type          TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}',
    status            TEXT        NOT NULL DEFAULT 'new',
    run_after         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    error             TEXT,
    last_heartbeat_at TIMESTAMPTZ,
    worker_id         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
    CONSTRAINT notasap_jobs_status_check CHECK (
        status IN ('new','running','done','failed','retrying','exhausted','cancelled')
    )
);

CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
    ON notasap_jobs (status, run_after)
    WHERE status IN ('new', 'retrying');

CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
    ON notasap_jobs (status, last_heartbeat_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
    ON notasap_jobs (status, updated_at)
    WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
```

`000001_notasap_jobs.down.sql`:

```sql
DROP TABLE IF EXISTS notasap_jobs;
```

</details>

<details>
<summary>Liquibase</summary>

```xml
<changeSet id="notasap-jobs-1" author="you">
    <sql splitStatements="true" endDelimiter=";">
        CREATE TABLE IF NOT EXISTS notasap_jobs (
            id                UUID        NOT NULL DEFAULT gen_random_uuid(),
            job_type          TEXT        NOT NULL,
            payload           JSONB       NOT NULL DEFAULT '{}',
            status            TEXT        NOT NULL DEFAULT 'new',
            run_after         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            attempt           INT         NOT NULL DEFAULT 0,
            max_attempts      INT         NOT NULL DEFAULT 3,
            error             TEXT,
            last_heartbeat_at TIMESTAMPTZ,
            worker_id         TEXT,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
            CONSTRAINT notasap_jobs_status_check CHECK (
                status IN ('new','running','done','failed','retrying','exhausted','cancelled')
            )
        );
        CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
            ON notasap_jobs (status, run_after)
            WHERE status IN ('new', 'retrying');
        CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
            ON notasap_jobs (status, last_heartbeat_at)
            WHERE status = 'running';
        CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
            ON notasap_jobs (status, updated_at)
            WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
    </sql>
    <rollback>
        DROP TABLE IF EXISTS notasap_jobs;
    </rollback>
</changeSet>
```

</details>

## Quick start

### 1. Describe your job payload

Any JSON-serializable type that implements `JobArgs`:

```go
type SendEmail struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

func (SendEmail) Kind() string { return "send_email" }
```

`Kind()` is the string stored in the `job_type` column.

### 2. Create a queue

```go
cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
    c.WorkerCount   = 10
    c.MaxRetries    = 5
    c.RetryStrategy = notasap.RetryStrategyExponential
})
if err != nil {
    log.Fatal(err)
}

q := notasap.NewQueue(pool, cfg)
```

### 3. Register a handler

```go
notasap.Register(q, func(ctx context.Context, job SendEmail) error {
    return mailer.Send(job.To, job.Subject)
})
```

The handler receives a decoded `SendEmail` value, not raw bytes.

### 4. Start and stop the worker pool

```go
q.Start()
defer q.Stop(context.Background())
```

### 5. Enqueue a job

```go
_, err := q.Enqueue(ctx, SendEmail{To: "a@b.co", Subject: "hi"})
```

## Transactional enqueue (outbox)

The main reason to pick a Postgres-backed queue is enqueueing a job in the same transaction as a business-data change. Either both commit, or neither does:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, "INSERT INTO orders (...) VALUES (...)"); err != nil {
    return err
}

if _, err := q.EnqueueTx(ctx, tx, SendEmail{To: "a@b.co"}); err != nil {
    return err
}

return tx.Commit(ctx)
```

The job becomes visible to workers only after `tx.Commit`.

## Batch enqueue

```go
err := q.EnqueueBatch(ctx,
    SendEmail{To: "a@b.co", Subject: "hi"},
    SendEmail{To: "c@d.co", Subject: "hey"},
)
```

`EnqueueBatchTx` is also available for transactional batch inserts.

## Cancel a job without retrying

Return `notasap.ErrCancel` from a handler — the job moves to terminal `cancelled` status immediately, remaining attempts are not used:

```go
notasap.Register(q, func(ctx context.Context, job SendEmail) error {
    if !userExists(job.To) {
        return notasap.ErrCancel
    }
    return mailer.Send(job.To, job.Subject)
})
```

Any other error triggers a normal retry according to the configured strategy.

## Configuration

### Global — `QueueConfig`

Passed to `NewQueueConfig`, applies to the whole queue.

| Field | Default | Purpose |
|-------|---------|---------|
| `WorkerCount` | `5` | Number of concurrent workers per instance |
| `PollInterval` | `2s` | Pause between DB polls when the queue is empty |
| `MaxRetries` | `3` | Max attempts before a job goes to `exhausted` |
| `RetryDelay` | `5s` | Base delay between attempts |
| `RetryStrategy` | `Exponential` | `RetryStrategyStatic` or `RetryStrategyExponential` |
| `MaxRetryDelay` | `10m` | Upper bound for exponential backoff |
| `HeartbeatInterval` | `15s` | How often a running job updates its heartbeat |
| `ShutdownTimeout` | `30s` | How long `Stop` waits for in-flight jobs |
| `Logger` | `slog.Default()` | Inject your own `*slog.Logger` |
| `Registerer` | `nil` | `prometheus.Registerer`; `nil` disables metrics |
| `Janitor` | see below | Automatic cleanup of terminal jobs |

### Per-job-type — `TaskConfig`

Passed as the third argument to `Register`, overrides global defaults. Zero-valued fields inherit from `QueueConfig`:

```go
notasap.Register(q,
    sendEmailHandler,
    func(tc *notasap.TaskConfig) {
        tc.MaxRetries    = 10
        tc.RetryStrategy = notasap.RetryStrategyStatic
        tc.RetryDelay    = 30 * time.Second
    },
)
```

| Field | Purpose |
|-------|---------|
| `MaxRetries` | Attempt limit for this job type |
| `RetryDelay` | Base delay |
| `RetryStrategy` | `Static` or `Exponential` |
| `MaxRetryDelay` | Upper bound on delay |

### Janitor

```go
cfg.Janitor = notasap.JanitorConfig{
    TTL:         24 * time.Hour,
    Statuses:    []notasap.Status{notasap.StatusDone, notasap.StatusCancelled},
    RunInterval: 1 * time.Hour,
    Disabled:    false,
}
```

| Field | Purpose |
|-------|---------|
| `TTL` | How long a terminal job lives before deletion |
| `Statuses` | Which statuses to clean (defaults: `done`, `exhausted`, `cancelled`) |
| `RunInterval` | How often the janitor runs |
| `Disabled` | `true` — disable cleanup entirely |

## Job statuses

Public constants — use these for filtering and checks:

| Status | When it's set |
|--------|---------------|
| `StatusNew` | Job has just been enqueued |
| `StatusRunning` | A worker has claimed the job |
| `StatusRetrying` | Attempt failed, waiting for the next one |
| `StatusDone` | Handler returned `nil` |
| `StatusCancelled` | Handler returned `ErrCancel` |
| `StatusExhausted` | All attempts used up |
| `StatusFailed` | Unrecoverable failure (panic, no handler registered) |

## Logging

Pass a `*slog.Logger` to `QueueConfig.Logger`. Inside a handler, fetch it back with `job_id`, `job_type`, `attempt` already attached:

```go
notasap.Register(q, func(ctx context.Context, job SendEmail) error {
    log := notasap.LoggerFromContext(ctx)
    log.Info("sending", "to", job.To)
    return mailer.Send(job.To, job.Subject)
})
```

## Prometheus metrics

Pass your own `prometheus.Registerer` in `QueueConfig.Registerer` — metrics register automatically. Leave it `nil` to disable metrics entirely.

```go
reg := prometheus.NewRegistry()
cfg.Registerer = reg
```

Exported metrics:

| Metric | Type | Labels |
|--------|------|--------|
| `notasap_jobs_enqueued_total` | Counter | `job_type` |
| `notasap_jobs_processed_total` | Counter | `job_type`, `status` |
| `notasap_job_duration_seconds` | Histogram | `job_type` |
| `notasap_jobs_in_flight` | Gauge | — |
| `notasap_poll_duration_seconds` | Histogram | — |

## Graceful shutdown

```go
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
<-sigCh

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := q.Stop(ctx); err != nil {
    log.Printf("shutdown: %v", err)
}
```

`Stop` stops claiming new jobs, waits for in-flight jobs up to `ShutdownTimeout`, and then cancels their contexts. Interrupted jobs go back to `retrying` and are picked up on the next poll.

## Playground

A self-contained demo with an HTMX UI, Prometheus, and Grafana is shipped under [`examples/playground/`](examples/playground/README.md). One `docker compose up` (or `make up` from that directory) brings up the full stack with a pre-provisioned dashboard.

## License

MIT
