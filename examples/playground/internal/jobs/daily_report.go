package jobs

import (
	"context"

	"github.com/google/uuid"

	"github.com/nottolstybai/not-asap/examples/playground/internal/simulator"
)

const KindDailyReport = "daily_report"

type DailyReportArgs struct {
	Date       string    `json:"date"`
	CustomerID uuid.UUID `json:"customer_id"`
}

func (DailyReportArgs) Kind() string { return KindDailyReport }

func DailyReportHandler(sim *simulator.Service) func(context.Context, DailyReportArgs) error {
	return func(ctx context.Context, _ DailyReportArgs) error {
		return sim.Call(ctx, KindDailyReport)
	}
}
