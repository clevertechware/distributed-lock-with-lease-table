package lease

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"time"
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

// Retention is how long an expired lease keeps its row before Migrate purges it.
const Retention = 24 * time.Hour

// purgeQuery deletes the leases nobody claimed for longer than the retention. A release keeps the row, so the key of
// a job that no longer exists would otherwise stay forever. The fence of a purged key starts again at 1: a downstream
// resource that remembers the last fence of that key needs a global sequence instead.
const purgeQuery = `
DELETE FROM lease
 WHERE expires_at < clock_timestamp() - make_interval(secs => $1)`

// Migrate creates the lease table and purges the leases expired for longer than Retention. Every replica runs it
// at startup, so it runs under a transaction-scoped advisory lock: the COMMIT releases it without pinning a
// connection.
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
	if _, err = tx.ExecContext(ctx, purgeQuery, Retention.Seconds()); err != nil {
		return fmt.Errorf("failed to purge expired leases: %w", err)
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
