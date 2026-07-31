package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clevertechware/distributed-lock-with-lease-table/internal/lease"
	"github.com/clevertechware/distributed-lock-with-lease-table/internal/pgtest"
)

const (
	tick        = 50 * time.Millisecond
	longEnough  = time.Second
	shortEnough = 300 * time.Millisecond
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

func TestOnlyOneOfTwoReplicasRunsTheJobAtATime(t *testing.T) {
	db, job := migratedPool(t), jobKey(t)
	work := &recorder{}

	ctx, stop := context.WithTimeout(t.Context(), longEnough)
	defer stop()

	run(t, ctx, newScheduler(db, "replica-1", job, work.job("replica-1", 3*tick)))
	run(t, ctx, newScheduler(db, "replica-2", job, work.job("replica-2", 3*tick)))

	require.Eventually(t, work.ranAtAll, longEnough, tick)
	<-ctx.Done()

	assert.Equal(t, 1, work.peak())
}

func TestTheStandbyReplicaTakesOverOnlyOnceTheHolderStops(t *testing.T) {
	db, job := migratedPool(t), jobKey(t)
	work := &recorder{}

	holderCtx, stopHolder := context.WithCancel(t.Context())
	defer stopHolder()

	run(t, holderCtx, newScheduler(db, "holder", job, work.job("holder", time.Minute)))
	run(t, t.Context(), newScheduler(db, "standby", job, work.job("standby", tick)))

	require.Eventually(t, ranBy(work, "holder"), longEnough, tick)
	require.Never(t, ranBy(work, "standby"), shortEnough, tick)

	stopHolder()

	assert.Eventually(t, ranBy(work, "standby"), longEnough, tick)
}

func TestAFailingJobStillGivesTheLockBack(t *testing.T) {
	db, job := migratedPool(t), jobKey(t)
	failing := func(context.Context) error { return errors.New("the job blew up") }

	newScheduler(db, "replica-1", job, failing).tick(t.Context())

	held, err := lease.NewLocker(db, lease.WithHolder("replica-2")).Lock(t.Context(), job)
	require.NoError(t, err)
	assert.NoError(t, held.Unlock(t.Context()))
}

func migratedPool(t *testing.T) *sql.DB {
	t.Helper()

	db, _ := server.Pool(t)
	require.NoError(t, lease.Migrate(t.Context(), db))

	return db
}

func jobKey(t *testing.T) string {
	t.Helper()

	return "nightly-reconciliation/" + t.Name()
}

func newScheduler(db *sql.DB, replica, key string, job Job) *Scheduler {
	locker := lease.NewLocker(db, lease.WithHolder(replica), lease.WithLogger(silent))

	return New(locker, key, job, WithInterval(tick), WithLogger(silent))
}

// run starts a replica and makes the test wait for it to stop, so that no tick outlives the pool the
// test cleanup closes.
func run(t *testing.T, ctx context.Context, scheduler *Scheduler) {
	t.Helper()

	var wg sync.WaitGroup
	wg.Go(func() { scheduler.Run(ctx) })
	t.Cleanup(wg.Wait)
}

func ranBy(work *recorder, replica string) func() bool {
	return func() bool { return slices.Contains(work.started(), replica) }
}

var silent = slog.New(slog.DiscardHandler)

// recorder stands for the resource the job writes to: it remembers who worked, and notices any two
// replicas working at the same time.
type recorder struct {
	mu      sync.Mutex
	running int
	highest int
	replica []string
}

// job builds the work of one replica. The duration is how long that replica occupies the job, unless
// its context ends first, which is what a replica being shut down mid-job does.
func (r *recorder) job(replica string, duration time.Duration) Job {
	return func(ctx context.Context) error {
		r.enter(replica)
		defer r.leave()

		select {
		case <-ctx.Done():
		case <-time.After(duration):
		}

		return nil
	}
}

func (r *recorder) enter(replica string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.running++
	r.highest = max(r.highest, r.running)
	r.replica = append(r.replica, replica)
}

func (r *recorder) leave() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.running--
}

func (r *recorder) peak() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.highest
}

func (r *recorder) started() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.replica)
}

func (r *recorder) ranAtAll() bool {
	return len(r.started()) > 0
}
