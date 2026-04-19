package jobs

import (
	"context"

	"github.com/google/uuid"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
)

const KindEmail = "email"

type EmailArgs struct {
	OrderID uuid.UUID `json:"order_id"`
	Address string    `json:"address"`
}

func (EmailArgs) Kind() string { return KindEmail }

func EmailHandler(sim *simulator.Service) func(context.Context, EmailArgs) error {
	return func(ctx context.Context, a EmailArgs) error {
		if a.Address == "invalid" {
			return notasap.ErrCancel
		}
		return sim.Call(ctx, KindEmail)
	}
}
