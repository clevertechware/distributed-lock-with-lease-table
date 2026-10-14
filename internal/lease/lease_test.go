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

func TestMigratePurgesALeaseExpiredForLongerThanTheRetention(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	insertLeaseExpiredSince(t, db, job, Retention+time.Hour)

	require.NoError(t, Migrate(ctx, db))

	assert.False(t, leaseExists(t, db, job))
}

func TestMigrateKeepsALeaseExpiredForLessThanTheRetentionAndItsFence(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	insertLeaseExpiredSince(t, db, job, Retention-time.Hour)

	require.NoError(t, Migrate(ctx, db))

	assert.Equal(t, int64(1), currentFence(t, db, job))
}

func TestMigrateKeepsALiveLease(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	claim(t, NewLocker(db, WithHolder("replica-1")), job)

	require.NoError(t, Migrate(ctx, db))

	assert.True(t, isLive(t, db, job))
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
	assert.True(t, isLive(t, db, job))
	assert.NoError(t, held.Unlock(ctx))
}

func TestClaimIsRefusedWhileAnotherReplicaHoldsTheLease(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	claim(t, NewLocker(db, WithHolder("replica-1")), job)

	_, err := NewLocker(db, WithHolder("replica-2")).Lock(ctx, job)

	assert.ErrorIs(t, err, lock.ErrAlreadyLocked)
}

func TestClaimingItsOwnValidLeaseAgainMovesTheFence(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	second := claim(t, locker, job)

	assert.Equal(t, first.Fence()+1, second.Fence())
	assert.NoError(t, second.Unlock(ctx))
}

func TestLateReleaseOfPreviousMandateLeavesTheReclaimedLeaseUntouched(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	second := claim(t, locker, job)

	assert.ErrorIs(t, first.Unlock(ctx), ErrLost)
	assert.Equal(t, second.Fence(), currentFence(t, db, job))
	assert.True(t, isLive(t, db, job))
}

func TestRenewByThePreviousMandateTouchesNoRowOnceTheLeaseIsReclaimed(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	second := claim(t, locker, job)
	abandonHeartbeat(first)

	assert.False(t, first.renew(ctx))
	assert.Equal(t, second.Fence(), currentFence(t, db, job))
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

func TestCleanReleaseThenReclaimMovesTheFence(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	require.NoError(t, first.Unlock(ctx))
	second := claim(t, locker, job)

	assert.Equal(t, int64(1), first.Fence())
	assert.Equal(t, int64(2), second.Fence())
}

func TestReplayedReleaseOfPreviousRunLeavesCurrentLeaseUntouched(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	require.NoError(t, first.Unlock(ctx))
	second := claim(t, locker, job)

	assert.ErrorIs(t, first.Unlock(ctx), ErrLost)
	assert.Equal(t, second.Fence(), currentFence(t, db, job))
	assert.True(t, isLive(t, db, job))
}

func TestReleasingALeaseShorterThanASecondRespectsTheValidityConstraint(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)

	held := claim(t, NewLocker(db, WithHolder("replica-1"), WithDuration(100*time.Millisecond)), job)

	assert.NoError(t, held.Unlock(ctx))
	assert.False(t, isLive(t, db, job))
}

func TestReclaimingAnExpiredLeaseInPlaceRespectsTheValidityConstraint(t *testing.T) {
	job := jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"), WithDuration(100*time.Millisecond))

	abandoned := claim(t, locker, job)
	abandonHeartbeat(abandoned)
	waitUntilExpired(t, db, job)
	reclaimed := claim(t, locker, job)

	assert.Equal(t, abandoned.Fence()+1, reclaimed.Fence())
	assert.True(t, currentExpiry(t, db, job).After(currentAcquiredAt(t, db, job)))
}

func TestDownstreamFenceAcceptsTheNewHolderAfterACleanRelease(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	downstream := newFencedResource(t, db)
	locker := NewLocker(db, WithHolder("replica-1"))

	first := claim(t, locker, job)
	require.True(t, downstream.accepts(t, first.Fence()))
	require.NoError(t, first.Unlock(ctx))
	second := claim(t, locker, job)

	assert.True(t, downstream.accepts(t, second.Fence()))
	assert.False(t, downstream.accepts(t, first.Fence()))
}

func TestReclaimWaitingForTheRowLockLongerThanTheLeaseRespectsTheValidityConstraint(t *testing.T) {
	ctx, job := t.Context(), jobKey(t)
	db := migratedPool(t)
	locker := NewLocker(db, WithHolder("replica-1"), WithDuration(shortLease))
	abandonHeartbeat(claim(t, locker, job))

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `SELECT 1 FROM lease WHERE key = $1 FOR UPDATE`, job)
	require.NoError(t, err)

	reclaimed := make(chan error, 1)
	go func() {
		held, lockErr := locker.Lock(ctx, job)
		if lockErr == nil {
			t.Cleanup(func() { _ = held.Unlock(context.Background()) })
		}
		reclaimed <- lockErr
	}()
	require.Eventually(t, func() bool { return claimIsWaitingForALock(t, db) }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(2 * shortLease)
	require.NoError(t, tx.Commit())

	assert.NoError(t, <-reclaimed)
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

func claimIsWaitingForALock(t *testing.T, db *sql.DB) bool {
	t.Helper()

	const query = `SELECT count(*) > 0 FROM pg_stat_activity
	               WHERE wait_event_type = 'Lock' AND query LIKE '%INSERT INTO lease%'`

	var waiting bool
	require.NoError(t, db.QueryRowContext(t.Context(), query).Scan(&waiting))

	return waiting
}

// insertLeaseExpiredSince writes the row a job leaves behind once nobody claims it anymore.
func insertLeaseExpiredSince(t *testing.T, db *sql.DB, key string, since time.Duration) {
	t.Helper()

	const insert = `
	INSERT INTO lease (key, holder, acquired_at, expires_at)
	VALUES ($1, 'replica-1',
	        clock_timestamp() - make_interval(secs => $2) - interval '1 minute',
	        clock_timestamp() - make_interval(secs => $2))`

	_, err := db.ExecContext(t.Context(), insert, key, since.Seconds())
	require.NoError(t, err)
}

func leaseExists(t *testing.T, db *sql.DB, key string) bool {
	t.Helper()

	var exists bool
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM lease WHERE key = $1)`, key).Scan(&exists))

	return exists
}

func isLive(t *testing.T, db *sql.DB, key string) bool {
	t.Helper()

	var live bool
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT expires_at > clock_timestamp() FROM lease WHERE key = $1`, key).Scan(&live))

	return live
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

func currentAcquiredAt(t *testing.T, db *sql.DB, key string) time.Time {
	t.Helper()

	var acquiredAt time.Time
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT acquired_at FROM lease WHERE key = $1`, key).Scan(&acquiredAt))

	return acquiredAt
}

// fencedResource stands for the downstream system a job writes to: it remembers the highest fence it has seen
// and rejects any write carrying a lower or equal one.
type fencedResource struct {
	db  *sql.DB
	key string
}

func newFencedResource(t *testing.T, db *sql.DB) *fencedResource {
	t.Helper()

	const ddl = `CREATE TABLE IF NOT EXISTS fenced_resource (key TEXT PRIMARY KEY, last_fence BIGINT NOT NULL)`
	require.NoError(t, execute(t, db, ddl))

	return &fencedResource{db: db, key: jobKey(t)}
}

func (r *fencedResource) accepts(t *testing.T, fence int64) bool {
	t.Helper()

	const write = `
INSERT INTO fenced_resource (key, last_fence) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET last_fence = EXCLUDED.last_fence WHERE fenced_resource.last_fence < EXCLUDED.last_fence`

	result, err := r.db.ExecContext(t.Context(), write, r.key, fence)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)

	return rows > 0
}

func execute(t *testing.T, db *sql.DB, query string) error {
	t.Helper()

	_, err := db.ExecContext(t.Context(), query)

	return err
}
