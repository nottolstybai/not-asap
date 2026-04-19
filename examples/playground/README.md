# not-asap playground

Interactive demo of the `not-asap` library: order-processing workload with simulated external services, live job table, tunable failure/latency knobs, Prometheus metrics, and a Grafana dashboard — all in one `docker compose up`.

## TL;DR

```bash
cd examples/playground
make up
# or: docker compose up --build

# open http://localhost:8080   — playground UI
# open http://localhost:3000   — Grafana (anonymous admin), "not-asap playground" dashboard
# open http://localhost:9091   — Prometheus UI
```

## What this shows

- `Queue.EnqueueTx` inside a business transaction (outbox pattern): `POST /orders` creates one `orders` row and 4 jobs in one commit.
- Exponential and static retry strategies per job type — visible in the `Next run` column of the jobs table.
- `ErrCancel`: set address to `invalid` and the `email` job goes straight to `cancelled`.
- `EnqueueBatch`: the "Enqueue batch" form does a single round-trip.
- Prometheus metrics (five library metrics) wired into Grafana.

## Scenarios

| Button | Library feature demonstrated |
|--------|------------------------------|
| Happy path | Smooth `EnqueueTx` flow, all four jobs reach `done`. |
| Payment flake | Exponential retry kicks in — watch `payment` attempts climb. |
| Invalid address | `ErrCancel` — email row becomes `cancelled` in one attempt. |
| Batch 100 | `EnqueueBatch` single round-trip for 100 daily reports. |

## Migrations

All SQL migrations (library schema + `orders` table) live under `migrations/` and are applied automatically on startup via a lightweight inline runner — no external migration tool required.

## Architecture

The playground is a standalone Go binary that uses `not-asap` the same way an external consumer would: import the library, create a pool, apply migrations, register handlers. The `simulator.Service` provides controllable latency and failure rates for each job type; its knobs are exposed in the UI.

## Grafana dashboard

Six panels: Enqueue rate, Processed rate by status, Jobs in flight, Job duration p50/p95/p99, Poll p95, Retry-exhaustion rate.

## Troubleshooting

- **Port 5433 already in use:** edit `docker-compose.yml` ports.
- **Grafana shows "No data":** wait ~15 s for Prometheus scrape; confirm `app:9090/metrics` returns metrics.
- **App container exits on start:** check `docker compose logs app` — most likely a migration conflict; run `make down` (drops volumes) and `make up` again.
