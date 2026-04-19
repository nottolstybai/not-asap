package web

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/examples/playground/internal/jobs"
	"github.com/nottolstybai/not-asap/examples/playground/internal/orders"
	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
)

type Handler struct {
	pool       *pgxpool.Pool
	queue      *notasap.Queue
	ordersRepo *orders.Repo
	sim        *simulator.Service
	mux        *http.ServeMux
}

func NewHandler(pool *pgxpool.Pool, q *notasap.Queue, ordersRepo *orders.Repo, sim *simulator.Service) *Handler {
	h := &Handler{pool: pool, queue: q, ordersRepo: ordersRepo, sim: sim, mux: http.NewServeMux()}
	h.routes()
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) routes() {
	h.mux.HandleFunc("GET /{$}", h.index)
	h.mux.HandleFunc("POST /orders", h.createOrder)
	h.mux.HandleFunc("POST /batch/reports", h.batchReports)
	h.mux.HandleFunc("POST /simulator/{kind}", h.updateSimulator)
	h.mux.HandleFunc("POST /scenarios/{name}", h.runScenario)
	h.mux.HandleFunc("GET /api/jobs", h.jobsFragment)
	h.mux.HandleFunc("GET /api/counters", h.countersFragment)
}

func (h *Handler) index(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildIndexData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := render(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type indexData struct {
	Knobs    map[string]simulator.Knob
	Jobs     []jobRow
	Counters map[string]int64
}

func (h *Handler) buildIndexData(ctx context.Context) (indexData, error) {
	knobs := h.sim.Snapshot()
	jobsRows, err := listJobs(ctx, h.pool, 50)
	if err != nil {
		return indexData{}, err
	}
	counters, err := jobCounters(ctx, h.pool)
	if err != nil {
		return indexData{}, err
	}
	return indexData{Knobs: knobs, Jobs: jobsRows, Counters: counters}, nil
}

func (h *Handler) createOrder(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	customer := r.FormValue("customer")
	address := r.FormValue("address")
	amount, _ := strconv.ParseFloat(r.FormValue("amount"), 64)
	if customer == "" || address == "" {
		http.Error(w, "customer and address required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	order, err := h.ordersRepo.Insert(ctx, tx, customer, address, amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.PaymentArgs{OrderID: order.ID, Amount: amount}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.EmailArgs{OrderID: order.ID, Address: address}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.InvoiceArgs{OrderID: order.ID}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.WarehouseArgs{OrderID: order.ID}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *Handler) batchReports(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n, _ := strconv.Atoi(r.FormValue("n"))
	if n <= 0 {
		n = 10
	}
	if n > 500 {
		n = 500
	}
	argsList := make([]core.JobArgs, n)
	for i := range argsList {
		argsList[i] = jobs.DailyReportArgs{
			Date:       time.Now().Format("2006-01-02"),
			CustomerID: uuid.New(),
		}
	}
	if err := h.queue.EnqueueBatch(r.Context(), argsList...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *Handler) updateSimulator(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	failure, _ := strconv.ParseFloat(r.FormValue("failure_rate"), 64)
	latMin, _ := strconv.Atoi(r.FormValue("latency_min_ms"))
	latMax, _ := strconv.Atoi(r.FormValue("latency_max_ms"))
	if failure < 0 {
		failure = 0
	}
	if failure > 1 {
		failure = 1
	}
	h.sim.Update(kind, simulator.Knob{
		FailureRate:  failure,
		LatencyMinMs: latMin,
		LatencyMaxMs: latMax,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) runScenario(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var fn func(context.Context, *Handler) error
	switch name {
	case "happy-path":
		fn = runHappyPath
	case "payment-flake":
		fn = runPaymentFlake
	case "invalid-address":
		fn = runInvalidAddress
	case "batch-100":
		fn = runBatch100
	default:
		http.Error(w, "unknown scenario", http.StatusNotFound)
		return
	}
	if err := fn(r.Context(), h); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *Handler) jobsFragment(w http.ResponseWriter, r *http.Request) {
	rowsList, err := listJobs(r.Context(), h.pool, 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = render(w, "_jobs_table", rowsList)
}

func (h *Handler) countersFragment(w http.ResponseWriter, r *http.Request) {
	c, err := jobCounters(r.Context(), h.pool)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = render(w, "_counters", c)
}

type jobRow struct {
	ID       string
	Kind     string
	Status   string
	Attempt  int
	NextRun  string
	UpdateAt string
}

func listJobs(ctx context.Context, pool *pgxpool.Pool, limit int) ([]jobRow, error) {
	rows, err := pool.Query(ctx, `
SELECT id, job_type, status, attempt, COALESCE(run_after::text, '-'), updated_at::text
FROM notasap_jobs
ORDER BY updated_at DESC
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []jobRow
	for rows.Next() {
		var r jobRow
		if err := rows.Scan(&r.ID, &r.Kind, &r.Status, &r.Attempt, &r.NextRun, &r.UpdateAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func jobCounters(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `SELECT status, COUNT(*) FROM notasap_jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}
