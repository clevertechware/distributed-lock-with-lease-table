package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/clevertechware/distributed-lock-with-lease-table/internal/advisory"
	"github.com/clevertechware/distributed-lock-with-lease-table/internal/lease"
	"github.com/clevertechware/distributed-lock-with-lease-table/internal/scheduler"
	"github.com/clevertechware/distributed-lock-with-lease-table/pkg/lock"
)

// defaultDSN points at the throwaway PostgreSQL started by `make demo-db`.
const defaultDSN = "postgres://demo:demo@localhost:5432/demo?sslmode=disable"

// mode is the implementation the replica runs under: the lease, or the session advisory lock it
// replaces. Running the demonstration twice, once in each mode, is the whole point of the binary.
type mode string

const (
	leaseMode    mode = "lease"
	advisoryMode mode = "advisory"
)

func parseMode(name string) (mode, error) {
	switch selected := mode(name); selected {
	case leaseMode, advisoryMode:
		return selected, nil
	default:
		return "", fmt.Errorf("unknown locker %q, expected %q or %q", name, leaseMode, advisoryMode)
	}
}

// config is the shape of the demonstration: everything is short, so that a takeover happens while
// the reader is still watching.
type config struct {
	name     string
	job      string
	locker   mode
	lease    time.Duration
	interval time.Duration
	work     time.Duration
}

/*
Command replica is one instance of an application whose job must run on a single replica at a time.

Start it twice against the same PostgreSQL, and the logs tell you which replica works, which one steps aside,
and what happens when the one that works goes away.
*/
func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With(slog.String("replica", cfg.name))

	if err = run(cfg, logger); err != nil {
		logger.Error("replica stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("replica stopped")
}

func parseFlags() (config, error) {
	cfg, locker := config{}, ""

	flag.StringVar(&cfg.name, "name", defaultName(), "identity of this replica, and holder of the lease")
	flag.StringVar(&cfg.job, "job", "invoices", "key of the job the replicas compete for")
	flag.StringVar(&locker, "locker", string(leaseMode), "lease, or advisory for the faulty implementation")
	flag.DurationVar(&cfg.lease, "lease", 6*time.Second, "how long a lease covers its holder")
	flag.DurationVar(&cfg.interval, "interval", 2*time.Second, "how often a replica tries to claim the job")
	flag.DurationVar(&cfg.work, "work", 5*time.Second, "how long the job takes")
	flag.Parse()

	selected, err := parseMode(locker)
	if err != nil {
		return config{}, err
	}
	cfg.locker = selected

	if err = cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (c config) validate() error {
	durations := []struct {
		flag  string
		value time.Duration
	}{
		{"lease", c.lease},
		{"interval", c.interval},
		{"work", c.work},
	}

	for _, duration := range durations {
		if duration.value <= 0 {
			return fmt.Errorf("-%s must be positive, got %s", duration.flag, duration.value)
		}
	}

	return nil
}

func run(cfg config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", dsn())
	if err != nil {
		return fmt.Errorf("failed to open the pool: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(3)
	db.SetConnMaxLifetime(2 * time.Minute)

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to reach postgres: %w", err)
	}
	locker, err := newLocker(ctx, cfg, db, logger)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "replica started",
		slog.String("job", cfg.job), slog.String("locker", string(cfg.locker)))

	taskScheduler := scheduler.New(
		locker,
		cfg.job,
		work(cfg.work),
		scheduler.WithInterval(cfg.interval),
		scheduler.WithLogger(logger),
	)
	taskScheduler.Run(ctx)

	return nil
}

func newLocker(ctx context.Context, cfg config, db *sql.DB, logger *slog.Logger) (lock.Locker, error) {
	if cfg.locker == advisoryMode {
		return advisory.NewSessionLocker(db), nil
	}

	if err := lease.Migrate(ctx, db); err != nil {
		return nil, fmt.Errorf("failed to migrate the lease table: %w", err)
	}

	return lease.NewLocker(
		db,
		lease.WithHolder(cfg.name),
		lease.WithDuration(cfg.lease),
		lease.WithLogger(logger),
	), nil
}

// work stands for the job that must not run twice at the same time. Sleeping is enough: what the
// demonstration shows is which replica runs it, not what it computes. It stops as soon as the replica
// is asked to shut down, so that the lease is handed back instead of being left to expire.
func work(duration time.Duration) scheduler.Job {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(duration):
			return nil
		}
	}
}

func dsn() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}

	return defaultDSN
}

// defaultName identifies this replica. The process id is what sets two replicas started on the same
// machine apart: sharing a holder would make them share the lease, which is reentrant by design, and
// the demonstration would show both of them working.
func defaultName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "replica"
	}

	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
