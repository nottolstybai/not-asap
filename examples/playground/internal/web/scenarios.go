package web

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/examples/playground/internal/jobs"
)

func runHappyPath(ctx context.Context, h *Handler) error {
	for _, k := range []string{jobs.KindPayment, jobs.KindEmail, jobs.KindInvoice, jobs.KindWarehouse} {
		knob := h.sim.Snapshot()[k]
		knob.FailureRate = 0
		h.sim.Update(k, knob)
	}
	for i := range 3 {
		if err := createOrderInline(ctx, h, fmt.Sprintf("hp-%d", i), "some-address", 42.00); err != nil {
			return err
		}
	}
	return nil
}

func runPaymentFlake(ctx context.Context, h *Handler) error {
	knob := h.sim.Snapshot()[jobs.KindPayment]
	knob.FailureRate = 0.70
	h.sim.Update(jobs.KindPayment, knob)
	for i := range 5 {
		if err := createOrderInline(ctx, h, fmt.Sprintf("pf-%d", i), "addr", 99.99); err != nil {
			return err
		}
	}
	return nil
}

func runInvalidAddress(ctx context.Context, h *Handler) error {
	return createOrderInline(ctx, h, "invalid-addr-customer", "invalid", 12.34)
}

func runBatch100(ctx context.Context, h *Handler) error {
	argsList := make([]core.JobArgs, 100)
	for i := range argsList {
		argsList[i] = jobs.DailyReportArgs{Date: "2026-04-19", CustomerID: uuid.New()}
	}
	return h.queue.EnqueueBatch(ctx, argsList...)
}

// createOrderInline mirrors createOrder but without HTTP plumbing.
func createOrderInline(ctx context.Context, h *Handler, customer, address string, amount float64) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	order, err := h.ordersRepo.Insert(ctx, tx, customer, address, amount)
	if err != nil {
		return err
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.PaymentArgs{OrderID: order.ID, Amount: amount}); err != nil {
		return err
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.EmailArgs{OrderID: order.ID, Address: address}); err != nil {
		return err
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.InvoiceArgs{OrderID: order.ID}); err != nil {
		return err
	}
	if _, err := h.queue.EnqueueTx(ctx, tx, jobs.WarehouseArgs{OrderID: order.ID}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Guard reference so notasap import isn't dropped by tooling.
var _ = notasap.ErrCancel
