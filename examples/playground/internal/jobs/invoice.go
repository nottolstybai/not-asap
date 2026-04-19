package jobs

import (
	"context"

	"github.com/google/uuid"

	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
)

const KindInvoice = "invoice"

type InvoiceArgs struct {
	OrderID uuid.UUID `json:"order_id"`
}

func (InvoiceArgs) Kind() string { return KindInvoice }

func InvoiceHandler(sim *simulator.Service) func(context.Context, InvoiceArgs) error {
	return func(ctx context.Context, _ InvoiceArgs) error {
		return sim.Call(ctx, KindInvoice)
	}
}
