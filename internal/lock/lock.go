// Package lock defines the contract expected by a scheduler that must run a job on a single replica.
package lock

import (
	"context"
	"errors"
)

// ErrAlreadyLocked is returned when another replica currently holds the lock for that key.
var ErrAlreadyLocked = errors.New("lock already held by another holder")

// Locker reserves a job for the calling replica.
type Locker interface {
	Lock(ctx context.Context, key string) (Lock, error)
}

// Lock is the reservation returned by a Locker, released once the job is done.
type Lock interface {
	Unlock(ctx context.Context) error
}
