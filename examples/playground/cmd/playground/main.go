package main

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/examples/playground/internal/jobs"
	"github.com/nottolstybai/not-asap/examples/playground/internal/orders"
	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
	"github.com/nottolstybai/not-asap/examples/playground/internal/web"
	pgmigrations "github.com/nottolstybai/not-asap/examples/playground/migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	dsn := env("DATABASE_URL", "postgres://notasap:notasap@localhost:5433/notasap_playground?sslmode=disable")
	httpAddr := env("HTTP_ADDR", ":8080")
	metricsAddr := env("METRICS_ADDR", ":9090")

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := applyMigrations(context.Background(), pool); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.Logger = logger
		c.Registerer = reg
		c.WorkerCount = 5
		c.PollInterval = 500 * time.Millisecond
		c.MaxRetries = 3
		c.RetryDelay = 2 * time.Second
		c.RetryStrategy = notasap.RetryStrategyExponential
	})
	if err != nil {
		log.Fatalf("NewQueueConfig: %v", err)
	}

	q := notasap.NewQueue(pool, cfg)

	sim := simulator.New(map[string]simulator.Knob{
		jobs.KindPayment:     {FailureRate: 0.40, LatencyMinMs: 100, LatencyMaxMs: 400},
		jobs.KindEmail:       {FailureRate: 0.10, LatencyMinMs: 50, LatencyMaxMs: 150},
		jobs.KindInvoice:     {FailureRate: 0.00, LatencyMinMs: 300, LatencyMaxMs: 600},
		jobs.KindWarehouse:   {FailureRate: 0.20, LatencyMinMs: 80, LatencyMaxMs: 200},
		jobs.KindDailyReport: {FailureRate: 0.05, LatencyMinMs: 200, LatencyMaxMs: 800},
	})

	registerHandlers(q, sim)

	ordersRepo := orders.NewRepo(pool)
	handler := web.NewHandler(pool, q, ordersRepo, sim)

	q.Start()

	metricsSrv := &http.Server{Addr: metricsAddr, Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})}
	go func() {
		logger.Info("metrics listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("metrics server: %v", err)
		}
	}()

	mainSrv := &http.Server{Addr: httpAddr, Handler: handler}
	go func() {
		logger.Info("http listening", "addr", httpAddr)
		if err := mainSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = mainSrv.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)
	if err := q.Stop(shutdownCtx); err != nil {
		logger.Warn("queue stop error", "error", err)
	}
}

func registerHandlers(q *notasap.Queue, sim *simulator.Service) {
	notasap.Register(q, jobs.PaymentHandler(sim), func(tc *notasap.TaskConfig) {
		tc.MaxRetries = 10
		tc.RetryStrategy = notasap.RetryStrategyExponential
		tc.RetryDelay = 2 * time.Second
		tc.MaxRetryDelay = 30 * time.Second
	})
	notasap.Register(q, jobs.EmailHandler(sim), func(tc *notasap.TaskConfig) {
		tc.MaxRetries = 3
		tc.RetryStrategy = notasap.RetryStrategyStatic
		tc.RetryDelay = 3 * time.Second
	})
	notasap.Register(q, jobs.InvoiceHandler(sim), func(tc *notasap.TaskConfig) {
		tc.MaxRetries = 3
		tc.RetryStrategy = notasap.RetryStrategyStatic
		tc.RetryDelay = 5 * time.Second
	})
	notasap.Register(q, jobs.WarehouseHandler(sim), func(tc *notasap.TaskConfig) {
		tc.MaxRetries = 5
		tc.RetryStrategy = notasap.RetryStrategyExponential
		tc.RetryDelay = 1 * time.Second
		tc.MaxRetryDelay = 15 * time.Second
	})
	notasap.Register(q, jobs.DailyReportHandler(sim), func(tc *notasap.TaskConfig) {
		tc.MaxRetries = 3
		tc.RetryStrategy = notasap.RetryStrategyStatic
		tc.RetryDelay = 10 * time.Second
	})
}

// applyMigrations runs all SQL files from the playground migrations embed.FS in lexical order,
// tracking applied filenames in a simple schema_migrations table.
// This avoids a goose library dependency in the playground binary.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS playground_schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return err
	}

	entries, err := fs.ReadDir(pgmigrations.Files, ".")
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		name := e.Name()

		var already bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM playground_schema_migrations WHERE filename=$1)`, name,
		).Scan(&already); err != nil {
			return err
		}
		if already {
			continue
		}

		data, err := pgmigrations.Files.ReadFile(name)
		if err != nil {
			return err
		}

		sql := extractGooseUp(string(data))
		if sql == "" {
			continue
		}

		if _, err := pool.Exec(ctx, sql); err != nil {
			return err
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO playground_schema_migrations (filename) VALUES ($1)`, name,
		); err != nil {
			return err
		}
		slog.Info("migration applied", "file", name)
	}
	return nil
}

// extractGooseUp extracts the SQL between "+goose Up" and "+goose Down" markers,
// concatenating each StatementBegin/StatementEnd block.
func extractGooseUp(content string) string {
	const upMarker = "-- +goose Up"
	const downMarker = "-- +goose Down"
	const beginMarker = "-- +goose StatementBegin"
	const endMarker = "-- +goose StatementEnd"

	start := strings.Index(content, upMarker)
	if start < 0 {
		return content // no goose markers, run as-is
	}
	content = content[start+len(upMarker):]

	if end := strings.Index(content, downMarker); end >= 0 {
		content = content[:end]
	}

	var result strings.Builder
	for {
		b := strings.Index(content, beginMarker)
		if b < 0 {
			break
		}
		content = content[b+len(beginMarker):]
		e := strings.Index(content, endMarker)
		if e < 0 {
			result.WriteString(content)
			break
		}
		result.WriteString(content[:e])
		content = content[e+len(endMarker):]
	}
	return result.String()
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
