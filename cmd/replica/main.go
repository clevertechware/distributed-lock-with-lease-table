// Command replica is one instance of an application whose job must run on a single replica at a
// time. Start it twice against the same PostgreSQL, and the logs tell you which replica works, which
// one steps aside, and what happens when the one that works goes away.
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

	"github.com/clevertechware/distributed-lock-with-lease-table/internal/lease"
	"github.com/clevertechware/distributed-lock-with-lease-table/internal/scheduler"
)

// defaultDSN points at the throwaway PostgreSQL started by `make demo-db`.
const defaultDSN = "postgres://demo:demo@localhost:5432/demo?sslmode=disable"

// config is the shape of the demonstration: everything is short, so that a takeover happens while
// the reader is still watching.
type config struct {
	name     string
	job      string
	lease    time.Duration
	interval time.Duration
	work     time.Duration
}

func main() {
	cfg := parseFlags()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With(slog.String("replica", cfg.name))

	if err := run(cfg, logger); err != nil {
		logger.Error("replica stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("replica stopped")
}

func parseFlags() config {
	cfg := config{}

	flag.StringVar(&cfg.name, "name", hostname(), "identity of this replica, and holder of the lease")
	flag.StringVar(&cfg.job, "job", "invoices", "key of the job the replicas compete for")
	flag.DurationVar(&cfg.lease, "lease", 6*time.Second, "how long a lease covers its holder")
	flag.DurationVar(&cfg.interval, "interval", 2*time.Second, "how often a replica tries to claim the job")
	flag.DurationVar(&cfg.work, "work", 5*time.Second, "how long the job takes")
	flag.Parse()

	return cfg
}

func run(cfg config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", dsn())
	if err != nil {
		return fmt.Errorf("failed to open the pool: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to reach postgres: %w", err)
	}
	if err = lease.Migrate(ctx, db); err != nil {
		return fmt.Errorf("failed to migrate the lease table: %w", err)
	}

	locker := lease.NewLocker(db,
		lease.WithHolder(cfg.name), lease.WithDuration(cfg.lease), lease.WithLogger(logger))

	logger.InfoContext(ctx, "replica started", slog.String("job", cfg.job), slog.Duration("lease", cfg.lease))

	scheduler.New(locker, cfg.job, work(cfg.work),
		scheduler.WithInterval(cfg.interval), scheduler.WithLogger(logger)).Run(ctx)

	return nil
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

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "replica"
	}

	return name
}
