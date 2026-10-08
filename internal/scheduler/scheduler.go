// Package scheduler runs a job on a tick, under a lock, the way a replica of an application does:
// every replica wakes up, asks whether the job is its turn, and works only if it got the lock.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/clevertechware/distributed-lock-with-lease-table/pkg/lock"
)

// DefaultInterval is how often a replica asks whether the job is its turn.
const DefaultInterval = 10 * time.Second

// claimErrorEscalateAfter is the rank of the consecutive claim failure that is logged at ERROR. The failures before
// it are logged at WARN: the loop claims again at the next tick, so a single failure heals before anyone could act.
const claimErrorEscalateAfter = 3

// releaseTimeout bounds the release that runs after the caller's context is already gone.
const releaseTimeout = 5 * time.Second

// Job is the work a replica performs once it holds the lock.
type Job func(ctx context.Context) error

// Scheduler is the loop of one replica: one locker, one job, one tick.
type Scheduler struct {
	locker   lock.Locker
	logger   *slog.Logger
	key      string
	interval time.Duration
	job      Job

	degradeClaimFailures bool
	claimFailures        int
}

// Option customises a Scheduler.
type Option func(*Scheduler)

// WithInterval sets how often the replica tries to claim the job.
func WithInterval(interval time.Duration) Option {
	return func(s *Scheduler) { s.interval = interval }
}

// WithLogger sets the logger reporting what happens at each tick.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Scheduler) { s.logger = logger }
}

// WithoutClaimFailureDegradation logs every claim failure at ERROR, instead of the first ones at WARN.
func WithoutClaimFailureDegradation() Option {
	return func(s *Scheduler) { s.degradeClaimFailures = false }
}

// New builds the scheduler of one replica for the given job key.
func New(locker lock.Locker, key string, job Job, opts ...Option) *Scheduler {
	scheduler := &Scheduler{
		locker:   locker,
		logger:   slog.Default(),
		key:      key,
		interval: DefaultInterval,
		job:      job,

		degradeClaimFailures: true,
	}
	for _, opt := range opts {
		opt(scheduler)
	}

	return scheduler
}

// Run ticks until the context is done, starting with an immediate attempt. Nothing that happens
// during a tick stops the loop: a replica that failed to claim the job, or failed the job itself,
// tries again at the next tick.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for ctx.Err() == nil {
		s.tick(ctx)

		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	held, err := s.locker.Lock(ctx, s.key)
	if errors.Is(err, lock.ErrAlreadyLocked) {
		s.claimFailures = 0
		s.logger.InfoContext(ctx, "job skipped, another replica holds it", slog.String("job", s.key))

		return
	}
	if err != nil {
		s.reportClaimFailure(ctx, err)

		return
	}
	s.claimFailures = 0
	defer s.release(ctx, held)

	s.logger.InfoContext(ctx, "job started", attributes(s.key, held)...)

	if err = s.job(ctx); err != nil {
		if ctx.Err() != nil {
			s.logger.InfoContext(ctx, "job interrupted by the shutdown", attributes(s.key, held)...)

			return
		}

		s.logger.ErrorContext(ctx, "job failed",
			append(attributes(s.key, held), slog.String("error", err.Error()))...)

		return
	}

	s.logger.InfoContext(ctx, "job done", attributes(s.key, held)...)
}

// reportClaimFailure logs a claim that failed for a reason other than another replica holding the lock. A canceled
// claim is a shutdown crossing the tick, not a failure, and ends the series like a successful one. A deadline overrun
// is deliberately counted: it reports a locker that is too slow.
func (s *Scheduler) reportClaimFailure(ctx context.Context, err error) {
	if errors.Is(err, context.Canceled) {
		s.claimFailures = 0

		return
	}
	if ctx.Err() != nil {
		return
	}

	s.claimFailures++
	level := slog.LevelError
	if s.degradeClaimFailures && s.claimFailures < claimErrorEscalateAfter {
		level = slog.LevelWarn
	}

	s.logger.Log(ctx, level, "failed to claim the job",
		slog.String("job", s.key), slog.Int("streak", s.claimFailures), slog.String("error", err.Error()))
}

// release gives the lock back on a context of its own: a shutdown in the middle of a job must still
// hand the job over, instead of leaving the next replica to wait for an expiry.
func (s *Scheduler) release(ctx context.Context, held lock.Lock) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()

	if err := held.Unlock(releaseCtx); err != nil {
		s.logger.ErrorContext(releaseCtx, "failed to release the job",
			slog.String("job", s.key), slog.String("error", err.Error()))
	}
}

// fenced is implemented by locks carrying a fencing token. The token belongs in the logs: it is what
// tells a takeover apart from a replica simply keeping the job it already had.
type fenced interface {
	Fence() int64
}

func attributes(key string, held lock.Lock) []any {
	attrs := []any{slog.String("job", key)}
	if withFence, ok := held.(fenced); ok {
		attrs = append(attrs, slog.Int64("fence", withFence.Fence()))
	}

	return attrs
}
