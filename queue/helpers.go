package queue

import (
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
)

// generateWorkerID creates a UUID v4 string for the worker instance identifier.
func generateWorkerID() string {
	return uuid.New().String()
}

// newSemaphore creates a semaphore with the given concurrency limit.
func newSemaphore(workerCount int) *semaphore.Weighted {
	return semaphore.NewWeighted(int64(workerCount))
}
