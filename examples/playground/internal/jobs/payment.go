package jobs

import (
	"context"

	"github.com/google/uuid"

	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
)

const KindPayment = "payment"

type PaymentArgs struct {
	OrderID uuid.UUID `json:"order_id"`
	Amount  float64   `json:"amount"`
}

func (PaymentArgs) Kind() string { return KindPayment }

func PaymentHandler(sim *simulator.Service) func(context.Context, PaymentArgs) error {
	return func(ctx context.Context, _ PaymentArgs) error {
		return sim.Call(ctx, KindPayment)
	}
}
