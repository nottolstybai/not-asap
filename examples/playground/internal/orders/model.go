package orders

import (
	"time"

	"github.com/google/uuid"
)

type Order struct {
	ID        uuid.UUID `db:"id"`
	Customer  string    `db:"customer"`
	Address   string    `db:"address"`
	Amount    float64   `db:"amount"`
	CreatedAt time.Time `db:"created_at"`
}
