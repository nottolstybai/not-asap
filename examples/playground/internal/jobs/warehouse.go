package jobs

import (
	"context"

	"github.com/google/uuid"

	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
)

const KindWarehouse = "warehouse"

type WarehouseArgs struct {
	OrderID uuid.UUID `json:"order_id"`
}

func (WarehouseArgs) Kind() string { return KindWarehouse }

func WarehouseHandler(sim *simulator.Service) func(context.Context, WarehouseArgs) error {
	return func(ctx context.Context, _ WarehouseArgs) error {
		return sim.Call(ctx, KindWarehouse)
	}
}
