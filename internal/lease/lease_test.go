package lease

import (
	"context"
	"database/sql"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clevertechware/distributed-lock-with-lease-table/pkg/lock"

	"github.com/clevertechware/distributed-lock-with-lease-table/internal/pgtest"
)

const shortLease = 300 * time.Millisecond

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

func TestMigrateRunsConcurrentlyOnEveryReplica(t *testing.T) {
	ctx := t.Context()

	var wg sync.WaitGroup
	for range 5 {
		db, _ := server.Pool(t)

		wg.Go(func() {
			assert.NoError(t, Migrate(ctx, db))
		})
	}
	wg.Wait()
}

func TestSchemaRejectsALeaseAlreadyExpiredAtBirth(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	const insert = `INSERT INTO lease (key, holder, expires_at) VALUES ($1, $2, clock_timestamp() - interval '1 second')`

	_, err := db.ExecContext(ctx, insert, job, "replica-1")

	assert.ErrorContains(t, err, "lease_validity")
}

func TestClaimGivesTheFirstFenceOnAFreeLease(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	held := claim(t, NewLocker(db, WithHolder("replica-1")), job)

	assert.Equal(t, int64(1), held.Fence())
	assert.True(t, held.ExpiresAt().After(time.Now()))
	assert.NoError(t, held.Unlock(ctx))
}

func TestClaimIsRefusedWhileAnotherReplicaHoldsTheLease(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	claim(t, NewLocker(db, WithHolder("replica-1")), job)

	_, err := NewLocker(db, WithHolder("replica-2")).Lock(ctx, job)

	assert.ErrorIs(t, err, lock.ErrAlreadyLocked)
}

func TestClaimingItsOwnValidLeaseAgainKeepsTheFence(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	second := claim(t, locker, job)

	assert.Equal(t, first.Fence(), second.Fence())
	assert.NoError(t, second.Unlock(ctx))
}

func TestClaimTakesOverAnExpiredLeaseAndMovesTheFence(t *testing.T) {
	job := jobKey(t)
	db := migratedPool(t)

	abandoned := claim(t, NewLocker(db, WithHolder("replica-1"), WithDuration(shortLease)), job)
	abandonHeartbeat(abandoned)
	waitUntilExpired(t, db, job)

	takenOver := claim(t, NewLocker(db, WithHolder("replica-2")), job)

	assert.Equal(t, abandoned.Fence()+1, takenOver.Fence())
}

func TestHeartbeatKeepsTheLeaseAliveWellBeyondItsDuration(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	held := claim(t, NewLocker(db, WithHolder("replica-1"), WithDuration(shortLease)), job)
	time.Sleep(3 * shortLease)

	_, err := NewLocker(db, WithHolder("replica-2")).Lock(ctx, job)

	assert.ErrorIs(t, err, lock.ErrAlreadyLocked)
	assert.True(t, currentExpiry(t, db, job).After(held.ExpiresAt()))
}

func TestRenewFailsOnceTheLeaseHasBeenTakenOver(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	abandoned := claim(t, NewLocker(db, WithHolder("replica-1"), WithDuration(shortLease)), job)
	abandonHeartbeat(abandoned)
	waitUntilExpired(t, db, job)
	claim(t, NewLocker(db, WithHolder("replica-2")), job)

	assert.False(t, abandoned.renew(ctx))
}

func TestUnlockLeavesTheLeaseOfTheReplicaThatTookOverUntouched(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	abandoned := claim(t, NewLocker(db, WithHolder("replica-1"), WithDuration(shortLease)), job)
	abandonHeartbeat(abandoned)
	waitUntilExpired(t, db, job)
	takenOver := claim(t, NewLocker(db, WithHolder("replica-2")), job)

	assert.ErrorIs(t, abandoned.Unlock(ctx), ErrLost)
	assert.Equal(t, takenOver.Fence(), currentFence(t, db, job))
}

func TestUnlockFromAPreviousRunOfTheSameReplicaLeavesTheCurrentLeaseUntouched(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"), WithDuration(shortLease))

	abandoned := claim(t, locker, job)
	abandonHeartbeat(abandoned)
	waitUntilExpired(t, db, job)
	reclaimed := claim(t, locker, job)

	require.Equal(t, abandoned.Fence()+1, reclaimed.Fence())
	assert.ErrorIs(t, abandoned.Unlock(ctx), ErrLost)
	assert.Equal(t, reclaimed.Fence(), currentFence(t, db, job))
}

func TestClaimingALeaseKeepsNoConnectionOutOfThePool(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	db.SetMaxOpenConns(1)

	claim(t, NewLocker(db, WithHolder("replica-1")), job)

	assert.NoError(t, db.PingContext(ctx))
}

func migratedPool(t *testing.T) *sql.DB {
	t.Helper()

	db, _ := server.Pool(t)
	require.NoError(t, Migrate(t.Context(), db))

	return db
}

func jobKey(t *testing.T) string {
	t.Helper()

	return "nightly-reconciliation/" + t.Name()
}

func claim(t *testing.T, locker *Locker, key string) *Lease {
	t.Helper()

	held, err := locker.Lock(t.Context(), key)
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Unlock(context.Background()) })

	return held.(*Lease)
}

// abandonHeartbeat stands for a replica that stops renewing without releasing: a crash, a long
// stop-the-world pause, or a network partition.
func abandonHeartbeat(held *Lease) {
	held.stop()
	<-held.stopped
}

func waitUntilExpired(t *testing.T, db *sql.DB, key string) {
	t.Helper()

	const query = `SELECT expires_at <= clock_timestamp() FROM lease WHERE key = $1`

	require.Eventually(t, func() bool {
		var expired bool
		require.NoError(t, db.QueryRowContext(t.Context(), query, key).Scan(&expired))

		return expired
	}, 2*time.Second, 20*time.Millisecond)
}

func currentFence(t *testing.T, db *sql.DB, key string) int64 {
	t.Helper()

	var fence int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT fence FROM lease WHERE key = $1`, key).Scan(&fence))

	return fence
}

func currentExpiry(t *testing.T, db *sql.DB, key string) time.Time {
	t.Helper()

	var expiresAt time.Time
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT expires_at FROM lease WHERE key = $1`, key).Scan(&expiresAt))

	return expiresAt
}
