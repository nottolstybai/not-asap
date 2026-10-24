<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img alt="notasap" src="docs/assets/logo-light.svg" width="380">
  </picture>
</p>

<p align="center">
  <b>Delayed background jobs for Go, stored in the Postgres you already run.</b>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/nottolstybai/not-asap"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/nottolstybai/not-asap.svg"></a>
  <img alt="Go 1.26+" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white">
  <img alt="PostgreSQL 14+" src="https://img.shields.io/badge/postgres-14%2B-4169E1?logo=postgresql&logoColor=white">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <b>English</b> · <a href="README.ru.md">Русский</a>
</p>

---

**notasap** runs background jobs inside your Go service, using one Postgres table as the queue.
There is no broker to deploy. Because a job is a row, you can schedule it **in the same transaction**
as your business data: the job exists only if the transaction commits.

```go
tx, _ := pool.Begin(ctx)
orderID := createOrder(ctx, tx, order)                       // your data
client.ScheduleTx(ctx, tx, SendInvoice{OrderID: orderID})    // your job
tx.Commit(ctx)                                               // both or neither
```

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Writing handlers](#writing-handlers)
- [Scheduling jobs](#scheduling-jobs)
- [Configuration](#configuration)
- [Running and stopping](#running-and-stopping)
- [Reliability](#reliability)
- [Observability](#observability)
- [Migrations](#migrations)
- [Development](#development)
- [License](#license)

## Features

- **Transactional scheduling.** `ScheduleTx` writes the job in your `pgx.Tx`.
- **Typed handlers.** Payloads are plain Go structs; handlers receive them already decoded.
- **Delays, retries, timeouts.** Start a job later, retry it with constant or exponential backoff, and cap each run with a timeout.
- **Idempotency keys.** Schedule the same logical job twice and get the first one back, or replace it on purpose.
- **Per-kind worker pools.** Every job kind has its own workers, so a slow kind never starves the others.
- **Crash recovery.** Jobs of a crashed process are picked up again; late results of a stale worker are rejected.
- **Graceful shutdown.** `Stop` drains running jobs; interrupted jobs go back to the queue without losing an attempt.
- **Housekeeping.** A janitor deletes old finished jobs on a per-status schedule.
- **Observability.** `log/slog` logging and an optional Prometheus collector.

## Quick start

### 1. Install

```sh
go get github.com/nottolstybai/not-asap
```

Requires Go 1.26+, PostgreSQL 14+ and [pgx v5](https://github.com/jackc/pgx).

### 2. Create the table

Apply the migration from [`migrations/`](migrations) with your migration tool
(see [Migrations](#migrations)).

### 3. Define a job and run the client

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	notasap "github.com/nottolstybai/not-asap"
)

// The kind number is stored in the database. Never change or reuse it.
const KindSendInvoice notasap.Kind = 1

type SendInvoice struct {
	OrderID int64 `json:"order_id"`
}

func (SendInvoice) Kind() notasap.Kind { return KindSendInvoice }

func sendInvoice(ctx context.Context, job *notasap.Job[SendInvoice]) error {
	notasap.Logger(ctx).Info("sending invoice", "order_id", job.Payload.OrderID)
	return nil // done
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, "postgres://localhost:5432/app")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	client, err := notasap.New(pool)
	if err != nil {
		log.Fatal(err)
	}
	err = notasap.Register(client, sendInvoice, notasap.KindConfig{Name: "send_invoice"})
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}

	_, err = client.Schedule(ctx, SendInvoice{OrderID: 42}, notasap.WithDelay(time.Minute))
	if err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = client.Stop(stopCtx)
}
```

A runnable version is in [`examples/basic`](examples/basic).

## How it works

Every job is a row in `notasap_jobs`. Each registered kind has a poller that claims
ready rows with `FOR UPDATE SKIP LOCKED` and hands them to that kind's workers.
Replicas of your service share the table and never claim the same row twice.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> pending: Schedule
    pending --> running: claimed by a worker
    running --> done: handler returns nil
    running --> pending: retry (attempts left),<br/>ErrKeepAttempt, shutdown
    running --> failed: attempts used up,<br/>payload cannot be decoded
    running --> cancelled: handler returns ErrCancel
    pending --> cancelled: client.Cancel(id)
    done --> [*]: janitor
    cancelled --> [*]: janitor
```

> [!NOTE]
> Delivery is **at least once**. A job can run more than once (for example after a crash),
> so handlers must be idempotent.

## Writing handlers

A handler is a function that receives the decoded payload and job metadata:

```go
func handle(ctx context.Context, job *notasap.Job[SendInvoice]) error
```

`job` carries `ID`, `Kind`, `Payload`, `Attempt` (starts at 1), `MaxAttempts` and `CreatedAt`.
`ctx` is cancelled when the kind's `Timeout` expires or the client stops, so pass it to every call
that can block. `notasap.Logger(ctx)` returns a logger with `job_id`, `kind` and `attempt` attached.

What the handler returns decides what happens next:

| Handler result | Job status | Attempt spent? |
|---|---|---|
| `nil` | `done` | yes |
| error wrapping `notasap.ErrCancel` | `cancelled` | yes |
| error wrapping `notasap.ErrKeepAttempt` | back to `pending` after backoff | **no** |
| any other error, a panic, or the timeout | `pending` after backoff, or `failed` if no attempts are left | yes |
| payload cannot be decoded | `failed` immediately | yes |
| interrupted by `Stop` | `pending`, runs again right away | **no** |

Sentinels are matched with `errors.Is`, so wrap them to keep a useful message:

```go
if !order.Paid {
	// The order is not paid yet. Check again later without spending an attempt.
	return fmt.Errorf("order %d not paid: %w", order.ID, notasap.ErrKeepAttempt)
}
```

> [!WARNING]
> `ErrKeepAttempt` has no limit: a job that always returns it lives forever.
> Stop retrying yourself, for example once `job.CreatedAt` is too old.
> Such retries are visible in the metric `notasap_jobs_retried_total{reason="keep_attempt"}`.

Error texts are stored in the `error` column, trimmed to 4 KB on a UTF-8 boundary.

## Scheduling jobs

```go
id, err := client.Schedule(ctx, payload, opts...)                    // standalone
id, err := client.ScheduleTx(ctx, tx, payload, opts...)              // inside your pgx.Tx
res, err := client.ScheduleBatch(ctx, notasap.Item(p1), notasap.Item(p2, opts...))
res, err := client.ScheduleBatchTx(ctx, tx, items...)
```

| Option | Effect |
|---|---|
| `notasap.WithDelay(d)` | the job becomes ready after `d` (database time) |
| `notasap.WithIdempotencyKey(key)` | at most one job per `(kind, key)` while the row exists |
| `notasap.WithReplace()` | with a key: overwrite the existing job instead of returning a duplicate |

Scheduling a kind that is not registered returns `ErrUnknownKind` and writes nothing.
A batch returns one `BatchResult{ID, Err}` per item; per-item `Err` is only `ErrDuplicate` or `ErrJobRunning`.

### Idempotency keys

When a job with the same kind and key already exists:

| Existing job | Without `WithReplace` | With `WithReplace` |
|---|---|---|
| `pending`, `done`, `failed`, `cancelled` | `ErrDuplicate` and the existing ID | the row is overwritten: new payload, `pending`, attempts reset, same ID |
| `running` | `ErrDuplicate` and the existing ID | `ErrJobRunning` and the existing ID |

The key is free again once the janitor deletes the old row. Failed jobs are kept forever by default,
so their keys stay taken until you replace them.

```go
id, err := client.Schedule(ctx, SendInvoice{OrderID: 42},
	notasap.WithIdempotencyKey("invoice:42"))
if errors.Is(err, notasap.ErrDuplicate) {
	// already scheduled; id points to the existing job
}
```

<details>
<summary>Locking details</summary>

- A batch of 2+ items with idempotency keys passed to `ScheduleBatch` runs in its own transaction, so it is all or nothing.
- `ScheduleTx` with `WithReplace` locks the existing row until your transaction ends, even when that row is `running`.
  Keep such transactions short: a worker that finishes the job meanwhile waits for the lock.

</details>

### Cancelling a pending job

```go
err := client.Cancel(ctx, id) // nil, ErrJobRunning, ErrJobFinished or ErrNotFound
```

Only `pending` jobs can be cancelled from outside. A running job stops only by its handler's decision.

## Configuration

Client options (all optional):

```go
client, err := notasap.New(pool,
	notasap.WithLogger(slog.Default()),
	notasap.WithObserver(metrics),                                  // see Observability
	notasap.WithDefaults(notasap.KindConfig{Workers: 10}),          // defaults for every kind
	notasap.WithRescuer(notasap.RescuerConfig{Interval: time.Minute}),
	notasap.WithJanitor(notasap.JanitorConfig{BatchSize: 500}),
	notasap.WithStopGrace(10*time.Second),
)
```

Per-kind settings in `KindConfig`. Zero fields fall back to `WithDefaults`, then to the built-in defaults:

| Field | Default | Meaning |
|---|---|---|
| `Name` | required | name used in logs and metrics |
| `Workers` | `5` | handlers running at the same time |
| `MaxAttempts` | `3` | attempts before the job is `failed` |
| `Timeout` | `1m` | limit for one handler run |
| `Backoff` | `Exponential(5s, 10m)` | delay before a retry (`Constant(d)`, `Exponential(base, max)` or your own func) |
| `PollInterval` | `1s` | how often an idle kind checks for ready jobs |

Background loops:

| Setting | Default | Meaning |
|---|---|---|
| `RescuerConfig.Interval` | `30s` | how often stuck jobs are looked for |
| `RescuerConfig.Grace` | `30s` | extra time after `Timeout` before a running job counts as stuck |
| `JanitorConfig.Interval` | `10m` | how often finished jobs are deleted |
| `JanitorConfig.BatchSize` | `1000` | rows per delete statement |
| `JanitorConfig.Retention` | done `24h`, cancelled `24h`, failed `0` | how long to keep each status; `0` = keep forever |
| `JanitorConfig.Disabled` | `false` | turn the janitor off (cannot be turned back on by a later `WithJanitor`) |
| `WithStopGrace` | `5s` | how long `Stop` waits for handlers to save their result after cancelling them |

`Retention` is merged per status: `Retention{StatusDone: time.Hour}` keeps the other defaults.
Invalid values are reported by `New` and `Register`, never as a panic at run time.

## Running and stopping

```go
err := client.Start(ctx) // checks the table exists, then starts workers and background loops
err := client.Stop(ctx)  // graceful shutdown
```

- `Register` every kind before `Start`. `Schedule` and `Cancel` work in any state.
- `Start` holds the client lock while it checks the schema, so give it a `ctx` with a deadline.
- A client starts once. A second `Start` returns `ErrAlreadyStarted`; `Start` after `Stop` returns `ErrStopped`.

`Stop` works in steps:

1. Stops claiming new jobs and stops the background loops.
   A claim already in flight finishes first (up to 10 s), and its jobs are run.
2. Waits for running handlers until `ctx` is done.
3. If `ctx` expires, cancels the handlers, waits `StopGrace` for them to save their result,
   and returns `ctx.Err()`. Interrupted jobs go back to `pending` without losing an attempt.

A second concurrent `Stop` waits for the first one, so `defer pool.Close()` is safe.

## Reliability

**Crashed processes.** When a worker claims a job it sets `deadline = now + Timeout + Grace`.
If the process dies, the rescuer (running in every replica) finds `running` jobs past their deadline and
returns them to `pending`, or marks them `failed` if no attempts are left. A crash spends an attempt,
so a job that keeps crashing the process cannot loop forever.

> [!IMPORTANT]
> A crashed job is retried only after `Timeout + Grace`. For long jobs use a long `Timeout`.

**Stale workers.** Every claim increments a `lease` counter. A worker saves its result only if the
lease still matches, so a worker that was too slow cannot overwrite the result of a newer run.

**Handlers that ignore `ctx`.** Go cannot stop a goroutine from outside. After `Timeout + Grace` the job
is reported as stuck (log and metric) and its worker slot stays busy until the handler returns.
Meanwhile the rescuer may give the job to another worker, in this process or another replica.

**Unregistered kinds.** Jobs of a kind that no replica registers stay `pending`.

## Observability

### Logs

Pass a `*slog.Logger` with `WithLogger`. Start and stop are logged at Info, finished jobs at Debug,
retries at Info, failures and panics at Error (with stack trace).

### Prometheus

```go
import "github.com/nottolstybai/not-asap/prom"

metrics, err := prom.New(prometheus.DefaultRegisterer,
	prom.WithJobDurationBuckets([]float64{0.1, 0.5, 1, 5, 30}),
)
client, err := notasap.New(pool, notasap.WithObserver(metrics))
```

| Metric | Labels |
|---|---|
| `notasap_jobs_scheduled_total` | `kind` |
| `notasap_jobs_finished_total` | `kind`, `status` (`done`, `failed`, `cancelled`) |
| `notasap_jobs_retried_total` | `kind`, `reason` (`error`, `panic`, `timeout`, `keep_attempt`, `shutdown`) |
| `notasap_job_duration_seconds` | `kind`, `outcome` |
| `notasap_jobs_running` | `kind` |
| `notasap_poll_duration_seconds` | `kind` |
| `notasap_jobs_rescued_total` | `kind` |
| `notasap_jobs_stuck_total` | `kind` |
| `notasap_janitor_deleted_total` | `status` |

`scheduled_total` is counted when the insert succeeds, so with `ScheduleTx` it also counts jobs
of transactions that are later rolled back.

To use another metrics system, implement the `notasap.Observer` interface
(embed `notasap.NopObserver` to override only what you need).

## Migrations

The schema ships in two formats, embedded in the [`migrations`](migrations) package:

| Tool | Files | Embedded FS |
|---|---|---|
| [goose](https://github.com/pressly/goose) | `migrations/goose/` | `migrations.Goose` |
| [golang-migrate](https://github.com/golang-migrate/migrate) | `migrations/golang-migrate/` | `migrations.GolangMigrate` |

Either copy the file into your own migrations directory, or run the embedded files with a
**separate version table**, so their numbers never clash with yours:

<details>
<summary>goose</summary>

```go
db := stdlib.OpenDBFromPool(pool) // github.com/jackc/pgx/v5/stdlib
goose.SetBaseFS(migrations.Goose)
goose.SetTableName("notasap_goose_version")
if err := goose.SetDialect("postgres"); err != nil { /* ... */ }
if err := goose.Up(db, "goose"); err != nil { /* ... */ }
```

</details>

<details>
<summary>golang-migrate</summary>

```go
src, _ := iofs.New(migrations.GolangMigrate, "golang-migrate")
m, _ := migrate.NewWithSourceInstance("iofs", src,
	"pgx5://localhost:5432/app?x-migrations-table=notasap_schema_migrations")
if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) { /* ... */ }
```

</details>

The table name is always `notasap_jobs`. To keep it in a separate schema, set `search_path` on the pool.

## Development

```sh
make test-unit   # fast tests, no database
make test        # all tests; starts Postgres with testcontainers (needs Docker)
make cover       # coverage report
make vet
```

Set `NOTASAP_TEST_DSN` to run the integration tests against your own database instead of a container.

## License

[MIT](LICENSE). Use it in any project, commercial or not.
