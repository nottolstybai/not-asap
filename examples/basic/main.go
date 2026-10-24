// Command basic shows the minimal notasap setup: register a handler, schedule
// a job and shut down gracefully.
//
//	DATABASE_URL=postgres://user:pass@localhost:5432/db go run ./examples/basic
//
// The notasap_jobs table must exist (see migrations/).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	notasap "github.com/nottolstybai/not-asap"
)

// KindSendInvoice is stored in the database: never change or reuse the number.
const KindSendInvoice notasap.Kind = 1

// SendInvoice is the payload of a send_invoice job.
type SendInvoice struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind implements notasap.Payload.
func (SendInvoice) Kind() notasap.Kind { return KindSendInvoice }

var errTemporary = errors.New("mail server is down")

func handleSendInvoice(ctx context.Context, job *notasap.Job[SendInvoice]) error {
	log := notasap.Logger(ctx)
	p := job.Payload

	switch {
	case p.Email == "":
		// Retrying cannot help: finish the job as cancelled.
		return fmt.Errorf("order %d has no email: %w", p.OrderID, notasap.ErrCancel)
	case job.Attempt == 1 && p.OrderID%2 == 0:
		// Retry later without spending an attempt.
		return fmt.Errorf("mail queue is full: %w", notasap.ErrKeepAttempt)
	case job.Attempt < 2 && p.OrderID%3 == 0:
		// Any other error is retried with backoff until MaxAttempts.
		return errTemporary
	}

	log.Info("invoice sent", "order_id", p.OrderID, "email", p.Email)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is not set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	client, err := notasap.New(pool)
	if err != nil {
		return err
	}
	err = notasap.Register(client, handleSendInvoice, notasap.KindConfig{
		Name:        "send_invoice",
		Workers:     4,
		MaxAttempts: 5,
		Timeout:     30 * time.Second,
		Backoff:     notasap.Exponential(time.Second, 10*time.Minute),
	})
	if err != nil {
		return err
	}

	if err := client.Start(ctx); err != nil {
		return err
	}

	id, err := client.Schedule(ctx, SendInvoice{OrderID: 42, Email: "user@example.com"},
		notasap.WithDelay(2*time.Second),
		notasap.WithIdempotencyKey("invoice-order-42"),
	)
	switch {
	case errors.Is(err, notasap.ErrDuplicate):
		log.Printf("job %d already scheduled", id)
	case err != nil:
		return err
	default:
		log.Printf("scheduled job %d", id)
	}

	<-ctx.Done()
	log.Println("shutting down")

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.Stop(stopCtx)
}
