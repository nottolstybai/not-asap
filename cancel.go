package notasap

import (
	"context"
	"fmt"

	"github.com/nottolstybai/not-asap/internal/store"
)

// Cancel cancels a pending job. It returns ErrJobRunning if the job is
// running, ErrJobFinished if it already reached a final status and
// ErrNotFound if it does not exist.
func (c *Client) Cancel(ctx context.Context, id int64) error {
	out, err := c.st.Cancel(ctx, id)
	if err != nil {
		return err
	}
	switch out {
	case store.CancelOK:
		return nil
	case store.CancelRunning:
		return ErrJobRunning
	case store.CancelFinished:
		return ErrJobFinished
	case store.CancelNotFound:
		return ErrNotFound
	}
	return fmt.Errorf("notasap: Cancel: unexpected outcome %d", out)
}
