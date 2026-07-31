package lease

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
)

const schema = `
CREATE TABLE IF NOT EXISTS lease
(
    key         TEXT        PRIMARY KEY,
    holder      TEXT        NOT NULL,
    fence       BIGINT      NOT NULL DEFAULT 1,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at  TIMESTAMPTZ NOT NULL,

    CONSTRAINT lease_validity CHECK (expires_at > acquired_at)
)`

// Migrate creates the lease table. Every replica runs it at startup, at the same time, which is
// exactly what a transaction-scoped advisory lock is for: the section is short and the lock is
// released by the COMMIT, without pinning a connection.
func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", migrationKey); err != nil {
		return fmt.Errorf("failed to acquire migration lock: %w", err)
	}
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("failed to create lease table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration: %w", err)
	}

	return nil
}

var migrationKey = func() int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte("lease.schema"))
	return int64(h.Sum32())
}()
