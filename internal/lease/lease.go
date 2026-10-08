// Package lease replaces the connection-scoped advisory lock by a row: a holder, an expiry and a
// fencing token. The connection goes back to the pool as soon as the claim is answered.
package lease

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/clevertechware/distributed-lock-with-lease-table/pkg/lock"
)

var (
	_ lock.Locker = (*Locker)(nil)
	_ lock.Lock   = (*Lease)(nil)
)

// ErrLost is returned when the lease is no longer held by us: it expired, or another replica
// took it over.
var ErrLost = errors.New("lease lost to another holder")

// DefaultDuration is how long a claim covers the holder. It is the only figure worth tuning: it
// bounds how long a job stays stuck after a replica dies brutally.
const DefaultDuration = 60 * time.Second

// claimQuery takes the lease when it is free, expired, or already ours, and does nothing otherwise. RETURNING
// yields no row when someone else holds a still valid lease, which is the answer, not an error. Every claim is a
// new mandate: fence and acquired_at move even when the same holder claims its own valid lease again, so that
// holder identifies the replica and fence identifies the mandate. A release or a renewal from a previous mandate
// then matches no row. acquired_at is read after the expiry computed in VALUES, which the lease_validity
// constraint tolerates for any duration longer than a few microseconds.
const claimQuery = `
INSERT INTO lease (key, holder, expires_at)
VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3))
ON CONFLICT (key) DO UPDATE
    SET holder      = EXCLUDED.holder,
        expires_at  = EXCLUDED.expires_at,
        acquired_at = clock_timestamp(),
        fence       = lease.fence + 1
WHERE lease.holder = EXCLUDED.holder OR lease.expires_at <= clock_timestamp()
RETURNING fence, expires_at`

// renewQuery reads as a single question: am I still the legitimate holder of this lease? An
// expired lease is not extended, because taking it back over has to move the fence.
const renewQuery = `
UPDATE lease
   SET expires_at = clock_timestamp() + make_interval(secs => $4)
 WHERE key    = $1
   AND holder = $2
   AND fence  = $3
   AND expires_at > clock_timestamp()`

// releaseQuery makes the lease expire in place instead of deleting the row, so that the fence survives a clean
// release and the next claim moves it. It is scoped to the holder and the fence, so that a late release cannot
// touch the lease of the replica that legitimately took over in the meantime, nor the one of a later run of the
// same replica. The expiry is set to now rather than to the past: the lease_validity constraint requires
// expires_at to stay after acquired_at.
const releaseQuery = `
UPDATE lease
   SET expires_at = clock_timestamp()
 WHERE key    = $1
   AND holder = $2
   AND fence  = $3
   AND expires_at > clock_timestamp()`

// Locker claims leases on behalf of one replica.
type Locker struct {
	db       *sql.DB
	logger   *slog.Logger
	holder   string
	duration time.Duration
}

// Option customises a Locker.
type Option func(*Locker)

// WithHolder overrides the generated holder identity.
func WithHolder(holder string) Option {
	return func(l *Locker) { l.holder = holder }
}

// WithDuration sets how long a claim covers the holder. The heartbeat interval follows as a third
// of it, so that three consecutive missed beats are needed to lose the lease.
func WithDuration(duration time.Duration) Option {
	return func(l *Locker) { l.duration = duration }
}

// WithLogger sets the logger used to report a lost lease.
func WithLogger(logger *slog.Logger) Option {
	return func(l *Locker) { l.logger = logger }
}

// NewLocker builds a locker over the given pool.
func NewLocker(db *sql.DB, opts ...Option) *Locker {
	locker := &Locker{
		db:       db,
		logger:   slog.Default(),
		holder:   newHolderID(),
		duration: DefaultDuration,
	}
	for _, opt := range opts {
		opt(locker)
	}

	return locker
}

// Holder returns the identity this locker claims leases under.
func (l *Locker) Holder() string {
	return l.holder
}

// Lock claims the lease for the given job, and starts the heartbeat that keeps it alive.
func (l *Locker) Lock(ctx context.Context, key string) (lock.Lock, error) {
	var (
		fence     int64
		expiresAt time.Time
	)

	row := l.db.QueryRowContext(ctx, claimQuery, key, l.holder, l.duration.Seconds())
	if err := row.Scan(&fence, &expiresAt); errors.Is(err, sql.ErrNoRows) {
		return nil, lock.ErrAlreadyLocked
	} else if err != nil {
		return nil, fmt.Errorf("failed to claim lease: %w", err)
	}

	held := &Lease{
		db:        l.db,
		logger:    l.logger,
		key:       key,
		holder:    l.holder,
		duration:  l.duration,
		fence:     fence,
		expiresAt: expiresAt,
		stopped:   make(chan struct{}),
	}

	// The heartbeat must outlive the caller's context: it runs until Unlock, not until the end of
	// the request that triggered the job.
	heartbeatCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	held.stop = stop
	go held.keepAlive(heartbeatCtx, l.duration/3)

	return held, nil
}

// Lease is a claimed lease, kept alive by its own goroutine until it is released.
type Lease struct {
	db        *sql.DB
	logger    *slog.Logger
	key       string
	holder    string
	duration  time.Duration
	fence     int64
	expiresAt time.Time
	stop      context.CancelFunc
	stopped   chan struct{}
}

// Fence is the fencing token of this claim. It has to travel with every write the job performs,
// so that the downstream resource can reject whatever arrives with a lower token.
func (l *Lease) Fence() int64 {
	return l.fence
}

// ExpiresAt is the moment the lease stops covering the holder, as decided by the database clock.
func (l *Lease) ExpiresAt() time.Time {
	return l.expiresAt
}

// Unlock stops the heartbeat and makes the lease expire, keeping its row and its fence for the next claim. It
// returns ErrLost when the lease already expired or changed hands.
func (l *Lease) Unlock(ctx context.Context) error {
	l.stop()
	<-l.stopped

	result, err := l.db.ExecContext(ctx, releaseQuery, l.key, l.holder, l.fence)
	if err != nil {
		return fmt.Errorf("failed to release lease: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read release outcome: %w", err)
	}
	if rows == 0 {
		return ErrLost
	}

	return nil
}

func (l *Lease) keepAlive(ctx context.Context, interval time.Duration) {
	defer close(l.stopped)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !l.renew(ctx) {
				return
			}
		}
	}
}

// renew extends the lease and reports whether the heartbeat should carry on. A transient failure
// is retried on the next beat, while zero row affected is final: the lease is gone.
func (l *Lease) renew(ctx context.Context) bool {
	result, err := l.db.ExecContext(ctx, renewQuery, l.key, l.holder, l.fence, l.duration.Seconds())
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		l.logger.WarnContext(ctx, "lease heartbeat failed, will retry",
			slog.String("job", l.key), slog.String("error", err.Error()))

		return true
	}

	rows, err := result.RowsAffected()
	if err != nil || rows > 0 {
		return true
	}

	l.logger.ErrorContext(ctx, "lease lost to another replica",
		slog.String("job", l.key), slog.String("holder", l.holder), slog.Int64("fence", l.fence))

	return false
}

// newHolderID builds the identity of this replica. The hostname is there for diagnosis, the random
// suffix guarantees that a restart produces a brand new identity.
func newHolderID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}

	return hostname + "-" + rand.Text()
}
