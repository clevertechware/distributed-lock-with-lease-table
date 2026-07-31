// Package advisory holds the two implementations discussed in the article: the session advisory
// lock taken through a pool, and the same lock taken on a connection pinned for the whole job.
package advisory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/clevertechware/distributed-lock-with-lease-table/internal/lock"
)

// ErrNotHeld is returned by PinnedLocker when PostgreSQL refuses to release a lock we do not own.
var ErrNotHeld = errors.New("advisory lock was not held by this connection")

const (
	tryLockQuery = "SELECT pg_try_advisory_lock($1)"
	unlockQuery  = "SELECT pg_advisory_unlock($1)"
)

// hashKey projects a business key onto the numeric key space advisory locks require.
// A 32-bit hash keeps the value within the positive range of a bigint, so the same key
// always maps to the same lock, whatever the replica and whatever the PostgreSQL version.
func hashKey(key string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int64(h.Sum32())
}

// SessionLocker is the faulty implementation. It acquires a session-scoped advisory lock through
// a *sql.DB, which is a pool and not a connection: the lock stays attached to the connection that
// took it, while the release runs on whichever connection the pool hands out next.
type SessionLocker struct {
	db *sql.DB
}

// NewSessionLocker builds a locker over the given pool.
func NewSessionLocker(db *sql.DB) *SessionLocker {
	return &SessionLocker{db: db}
}

// Lock tries to reserve the job. If another replica already holds it,
// we return ErrAlreadyLocked and the scheduler simply skips its turn.
func (l *SessionLocker) Lock(ctx context.Context, key string) (lock.Lock, error) {
	hash := hashKey(key)

	var acquired bool
	if err := l.db.QueryRowContext(ctx, tryLockQuery, hash).Scan(&acquired); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		return nil, lock.ErrAlreadyLocked
	}

	return &sessionLock{db: l.db, hash: hash}, nil
}

type sessionLock struct {
	db   *sql.DB
	hash int64
}

// Unlock is called by the scheduler once the job is done. It borrows an arbitrary connection from
// the pool and discards the boolean PostgreSQL returns, so a release that never happened looks
// exactly like a successful one.
func (l *sessionLock) Unlock(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx, unlockQuery, l.hash)
	return err
}

// PinnedLocker is the first fix: the connection that acquires the lock is taken out of the pool
// and kept until the job releases it. The semantics are correct, at the cost of one connection
// held for the whole duration of the job.
type PinnedLocker struct {
	db *sql.DB
}

// NewPinnedLocker builds a locker over the given pool.
func NewPinnedLocker(db *sql.DB) *PinnedLocker {
	return &PinnedLocker{db: db}
}

// Lock reserves a dedicated connection, then acquires the lock on it.
func (l *PinnedLocker) Lock(ctx context.Context, key string) (lock.Lock, error) {
	hash := hashKey(key)

	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to reserve a connection: %w", err)
	}

	var acquired bool
	if err = conn.QueryRowContext(ctx, tryLockQuery, hash).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, lock.ErrAlreadyLocked
	}

	return &pinnedLock{conn: conn, hash: hash}, nil
}

type pinnedLock struct {
	conn *sql.Conn
	hash int64
}

// Unlock releases the lock on the very connection that took it, and gives that connection back
// to the pool.
func (l *pinnedLock) Unlock(ctx context.Context) error {
	defer func() { _ = l.conn.Close() }()

	var released bool
	if err := l.conn.QueryRowContext(ctx, unlockQuery, l.hash).Scan(&released); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if !released {
		return ErrNotHeld
	}

	return nil
}
