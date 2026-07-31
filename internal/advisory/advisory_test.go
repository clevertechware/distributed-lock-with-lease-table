package advisory

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clevertechware/distributed-lock-with-lease-table/pkg/lock"

	"github.com/clevertechware/distributed-lock-with-lease-table/internal/pgtest"
)

var server *pgtest.Server

func TestMain(m *testing.M) {
	ctx := context.Background()

	started, err := pgtest.Start(ctx)
	if err != nil {
		log.Fatalf("failed to start postgres: %v", err)
	}
	server = started

	code := m.Run()

	if err = server.Stop(ctx); err != nil {
		log.Printf("failed to stop postgres: %v", err)
	}
	os.Exit(code)
}

func TestSessionLockerLeavesTheLockBehindWhenUnlockRunsOnAnotherConnection(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db, notices := server.Pool(t)

	held, err := NewSessionLocker(db).Lock(ctx, job)
	require.NoError(t, err)

	busy := holdIdleConnection(t, db)
	defer func() { _ = busy.Close() }()

	require.NoError(t, held.Unlock(ctx))

	assert.True(t, advisoryLockIsHeld(t, db, job), "the lock is still attached to the connection that took it")
	assert.Contains(t, strings.Join(notices.All(), "\n"), "you don't own a lock of type ExclusiveLock")
}

func TestOrphanedLockIsSilentlyReleasedWhenThePoolRecyclesTheConnection(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	replicaOne, _ := server.Pool(t)
	replicaTwo, _ := server.Pool(t)

	_, err := NewSessionLocker(replicaOne).Lock(ctx, job)
	require.NoError(t, err)
	require.True(t, advisoryLockIsHeld(t, replicaTwo, job))

	recycleIdleConnections(replicaOne)

	require.False(t, advisoryLockIsHeld(t, replicaTwo, job), "closing the connection released the lock, without a word")

	_, err = NewSessionLocker(replicaTwo).Lock(ctx, job)
	assert.NoError(t, err, "the second replica now runs the very same job, while the first one still believes it holds the lock")
}

func TestPinnedLockerReleasesTheLockItAcquired(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db, notices := server.Pool(t)

	held, err := NewPinnedLocker(db).Lock(ctx, job)
	require.NoError(t, err)

	busy := holdIdleConnection(t, db)
	defer func() { _ = busy.Close() }()

	require.NoError(t, held.Unlock(ctx))

	assert.False(t, advisoryLockIsHeld(t, db, job))
	assert.Empty(t, notices.All())
}

func TestPinnedLockerRejectsASecondReplica(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	replicaOne, _ := server.Pool(t)
	replicaTwo, _ := server.Pool(t)

	_, err := NewPinnedLocker(replicaOne).Lock(ctx, job)
	require.NoError(t, err)

	_, err = NewPinnedLocker(replicaTwo).Lock(ctx, job)
	assert.ErrorIs(t, err, lock.ErrAlreadyLocked)
}

func TestPinnedLockerKeepsItsConnectionOutOfThePoolForTheWholeJob(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db, _ := server.Pool(t)
	db.SetMaxOpenConns(1)

	_, err := NewPinnedLocker(db).Lock(ctx, job)
	require.NoError(t, err)

	starved, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err = db.PingContext(starved)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "the only connection of the pool is confiscated until the job ends, got %v", err)
}

// jobKey keeps every test on its own advisory lock, so that a lock deliberately left behind by one
// test cannot leak into the next.
func jobKey(t *testing.T) string {
	t.Helper()

	return "nightly-reconciliation/" + t.Name()
}

// holdIdleConnection takes the connection sitting idle in the pool, so that the next statement is
// forced onto another one. It reproduces, deterministically, what concurrent traffic does to a pool.
func holdIdleConnection(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)

	return conn
}

// recycleIdleConnections closes every idle connection, as the pool does on its own once
// ConnMaxLifetime is reached.
func recycleIdleConnections(db *sql.DB) {
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(2)
}

func advisoryLockIsHeld(t *testing.T, db *sql.DB, key string) bool {
	t.Helper()

	const query = `SELECT count(*) > 0 FROM pg_locks WHERE locktype = 'advisory' AND classid = 0 AND objid::bigint = $1`

	var held bool
	require.NoError(t, db.QueryRowContext(t.Context(), query, hashKey(key)).Scan(&held))

	return held
}
