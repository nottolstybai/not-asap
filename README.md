# not-asap

Delayed background jobs on top of PostgreSQL, for Go. No broker: just the database your service already has.

*На русском: [README.ru.md](README.ru.md).*

- Type-safe handlers via generics; payloads are JSON.
- Schedule inside your own transaction (`ScheduleTx`) for the outbox pattern.
- Delayed jobs, retries with backoff, idempotency keys, cancellation.
- One worker pool per kind, a rescuer for stuck jobs, a janitor for finished ones.
- Graceful shutdown; optional Prometheus metrics in the `prom` subpackage.

Requirements: Go 1.26+, PostgreSQL 14+, `*pgxpool.Pool` from [pgx v5](https://github.com/jackc/pgx).

```bash
go get github.com/nottolstybai/not-asap
```

## Migrations

The schema is one table, `notasap_jobs`. Either copy a file into your own migrations directory, or apply the embedded FS with a **separate version table**, so numbering does not collide with your service's migrations.

- Files: `migrations/goose/00001_notasap_jobs.sql`, `migrations/golang-migrate/000001_notasap_jobs.{up,down}.sql`.
- Embedded: `migrations.Goose` and `migrations.GolangMigrate` (package `github.com/nottolstybai/not-asap/migrations`).

goose:

```go
goose.SetBaseFS(migrations.Goose)
goose.SetTableName("notasap_goose_version")
err := goose.Up(db, "goose") // db is *sql.DB, e.g. from stdlib.OpenDBFromPool(pool)
```

or, with the provider API, `goose.NewProvider(goose.DialectPostgres, db, sub, goose.WithTableName("notasap_goose_version"))` where `sub, _ := fs.Sub(migrations.Goose, "goose")`.

golang-migrate:

```go
src, _ := iofs.New(migrations.GolangMigrate, "golang-migrate")
m, _ := migrate.NewWithSourceInstance("iofs", src,
    dsn+"&x-migrations-table=notasap_schema_migrations") // dsn already has a query string
err := m.Up()
```

All SQL is unqualified, so isolate the table with the connection's `search_path` if needed. `Start` fails with a clear error if the table is missing.

## Quick start

A complete program is in [examples/basic](examples/basic/main.go).

```go
const KindSendInvoice notasap.Kind = 1 // stored in the DB: never change or reuse

type SendInvoice struct {
    OrderID int64  `json:"order_id"`
    Email   string `json:"email"`
}

func (SendInvoice) Kind() notasap.Kind { return KindSendInvoice }

client, err := notasap.New(pool)
err = notasap.Register(client, handle, notasap.KindConfig{Name: "send_invoice"})
err = client.Start(ctx) // does not block

id, err := client.Schedule(ctx, SendInvoice{OrderID: 42, Email: "a@b.c"},
    notasap.WithDelay(time.Minute),
    notasap.WithIdempotencyKey("invoice-42"))

err = client.Stop(shutdownCtx)
```

```go
func handle(ctx context.Context, job *notasap.Job[SendInvoice]) error {
    notasap.Logger(ctx).Info("sending", "order", job.Payload.OrderID) // logger has job_id, kind, attempt
    return send(ctx, job.Payload)
}
```

Kinds are numbers (`int16`, must be > 0) stored in the database. The `KindConfig.Name` is required, unique, and used only in logs and metrics. Payload types must be non-pointer, non-interface types. Registering must happen before `Start`.

The client lifecycle is single-use: `new -> started -> stopped`. A second `Start` returns `ErrAlreadyStarted`, `Start` after `Stop` returns `ErrStopped`, `Stop` on a client that is not running returns `nil`, but still moves it to stopped, so a later `Start` returns `ErrStopped`. A repeated or concurrent `Stop` waits until the first `Stop` returns (or its own `ctx` is done, then it returns `ctx.Err()`). `Start` holds the client lock during its schema check, so pass it a `ctx` with a deadline. `Schedule*` and `Cancel` work in any state, they only write to the database.

## Handler outcomes

| Handler result | What happens |
|---|---|
| `nil` | `done` |
| error wrapping `ErrCancel` | `cancelled`, the error text is stored |
| error wrapping `ErrKeepAttempt` | back to `pending`, the attempt is **not** spent, `run_at = NOW() + Backoff(job.Attempt)`. No limit: use with care, watch `reason=keep_attempt` |
| any other error, panic, kind `Timeout` | `attempt >= MaxAttempts`: `failed`; otherwise `pending` with `run_at = NOW() + Backoff(attempt)`. The error is stored; a panic is logged with a stack |
| payload cannot be decoded | `failed` at once (retrying is pointless) |
| interrupted by shutdown (handler returned an error) | `pending`, the attempt is refunded, `run_at = NOW()` |
| returned `nil` despite shutdown | `done` |

Error text is truncated to 4096 bytes on a rune boundary.

An attempt is spent at claim time, so a process crash also costs an attempt (protection against poison jobs). Delivery is **at-least-once**: a job can run more than once (crash, rescue, shutdown), so handlers must be idempotent.

## Idempotency

`WithIdempotencyKey(key)` makes `(kind, key)` unique while the row exists. Rows leave through the janitor, so a key is freed when a `done`/`cancelled` row is deleted (a `failed` row is kept forever by default).

| Existing row | without `WithReplace` | with `WithReplace` |
|---|---|---|
| none | inserted | inserted |
| `pending`, `done`, `failed`, `cancelled` | `ErrDuplicate` + ID of the existing job | the same row is **overwritten** (ID kept): new payload, `pending`, `attempt=0`, `max_attempts` from current config, no error, new `run_at` |
| `running` | `ErrDuplicate` + ID | `ErrJobRunning` + ID |

`WithReplace` requires `WithIdempotencyKey`. In both cases the returned ID is the existing job's.

Notes:

- `ScheduleTx` with `WithReplace` locks the matched row until your transaction ends, also when the row is running and `ErrJobRunning` is returned. A long transaction then blocks the worker's status write (10s timeout); the write fails, the job is rescued after its deadline and runs again. Keep such transactions short.
- Plain duplicates do not lock the existing row.
- `ScheduleBatch` (no tx) with idempotency keys runs in its own transaction (for batches of 2+ items), because resolving conflicts may take more round trips; without keys it is one round trip.

## Scheduling

```go
id, err := client.Schedule(ctx, p, opts...)
id, err := client.ScheduleTx(ctx, tx, p, opts...)          // visible when tx commits
res, err := client.ScheduleBatch(ctx, notasap.Item(p1, opts...), notasap.Item(p2))
res, err := client.ScheduleBatchTx(ctx, tx, items...)
```

- `WithDelay(d)`: `run_at = NOW() + d`, using the database clock.
- A kind without a registered handler gives `ErrUnknownKind`, nothing is written.
- Batches are atomic. Per-item `ErrDuplicate`/`ErrJobRunning` are in `BatchResult{ID, Err}`; any other error fails the whole batch.
- `max_attempts` is fixed in the row from the kind's config at scheduling time.
- The `scheduled_total` metric is counted after the statement succeeds, before your transaction commits, so it overcounts if the `*Tx` caller rolls back.

## Cancel

```go
err := client.Cancel(ctx, id)
```

`pending -> cancelled`. Returns `ErrJobRunning` for a running job, `ErrJobFinished` if it is already done/failed/cancelled, `ErrNotFound` if there is no such row. To stop a running job from the inside, return `ErrCancel` from the handler.

## Configuration

Zero fields fall back to `WithDefaults`, then to built-in defaults.

| Option | Field | Default |
|---|---|---|
| `KindConfig` | `Workers` | 5 |
| | `MaxAttempts` | 3 |
| | `Timeout` | 1m |
| | `Backoff` | `Exponential(5s, 10m)` |
| | `PollInterval` | 1s |
| `WithRescuer(RescuerConfig)` | `Interval` | 30s |
| | `Grace` | 30s |
| `WithJanitor(JanitorConfig)` | `Interval` | 10m |
| | `BatchSize` | 1000 |
| | `Retention` | done 24h, cancelled 24h, failed 0 (keep) |
| | `Disabled` | false |
| `WithStopGrace(d)` | | 5s |
| `WithLogger(*slog.Logger)` | | `slog.Default()` |
| `WithObserver(Observer)` | | no-op |

- `WithDefaults(KindConfig{...})` sets client-wide kind defaults (its `Name` must be empty).
- Backoff: `Constant(d)`, `Exponential(base, max)` (doubling, jitter in `[d/2, d]`, overflow-safe), or your own `func(attempt int) time.Duration`. The constructors panic on invalid arguments, like `regexp.MustCompile`; call them at setup.
- Janitor `Retention` is merged per key over the defaults: `{StatusDone: time.Hour}` keeps `cancelled` at 24h. Use an explicit `0` to never delete a status. Only done, failed and cancelled are allowed.
- `Disabled` is sticky: once any `WithJanitor` sets it, a later `WithJanitor(JanitorConfig{})` does not re-enable it.
- All configuration is validated in `New`/`Register` and returned as an error.

## Rescuer and long jobs

A claimed job gets `deadline = claim + Timeout + Rescuer.Grace`. The rescuer (always on) returns running jobs past their deadline to `pending` with the kind's backoff, or to `failed` if attempts are exhausted (`error = "rescued: deadline exceeded"`). All instances of a service share this, and a late write from the old worker is rejected by a lease counter (fencing).

There is no heartbeat, so a job lost with a crashed process comes back no sooner than `Timeout + Grace`. For long jobs set a large `Timeout`.

A handler that ignores `ctx` keeps its worker slot. Once the rescuer returns its job to `pending`, any free slot, in this process or another, may claim it again while the stale handler still runs; the stale handler's result is rejected by the lease. If it has not returned `Rescuer.Grace` after `Timeout`, the pool logs an error and reports `stuck`. The slot stays busy until it returns.

Jobs of a kind that is not registered in a process stay `pending` there and are not claimed by that process; another process that registers the kind will take them.

## Shutdown

`Stop(ctx)`:

1. stops polling, the rescuer and the janitor;
2. waits for running handlers until `ctx` is done;
3. if `ctx` expires, cancels handler contexts (cause: shutdown), waits `StopGrace` for them to record the status, and returns `ctx.Err()`.

Interrupted jobs go back to `pending` with their attempt refunded. If `Stop` times out, handlers that ignore their context may still be running after it returns; their jobs are left to the rescuer.

## Metrics

The core has no dependencies; it reports to an `Observer`. The `prom` subpackage implements it for Prometheus:

```go
m, err := prom.New(prometheus.DefaultRegisterer,
    prom.WithJobDurationBuckets([]float64{0.1, 1, 10, 60}),
    prom.WithPollDurationBuckets([]float64{0.001, 0.01, 0.1}),
)
client, err := notasap.New(pool, notasap.WithObserver(m))
```

`prom.New` returns an error (no panic) on a nil registerer, duplicate registration or invalid buckets. Defaults: job duration `ExponentialBuckets(0.005, 2, 15)`, poll `ExponentialBuckets(0.0005, 2, 12)`. The `kind` label is `KindConfig.Name`.

| Metric | Labels |
|---|---|
| `notasap_jobs_scheduled_total` | kind |
| `notasap_jobs_finished_total` | kind, status (done/failed/cancelled) |
| `notasap_jobs_retried_total` | kind, reason (error/panic/timeout/keep_attempt/shutdown) |
| `notasap_job_duration_seconds` | kind, outcome (done/failed/cancelled/retried) |
| `notasap_jobs_running` | kind |
| `notasap_poll_duration_seconds` | kind |
| `notasap_jobs_rescued_total` | kind |
| `notasap_jobs_stuck_total` | kind |
| `notasap_janitor_deleted_total` | status |

To export elsewhere, implement `notasap.Observer`; embed `notasap.NopObserver` to override only some methods. Implementations must be concurrency-safe and not block.

## Development

```bash
make test    # go test -race ./... (integration tests need Docker)
make cover   # coverage across packages
make vet
```

Integration tests use testcontainers (`postgres:16-alpine`) and are skipped under `-short`.
