// Package pgtest starts a throwaway PostgreSQL instance and hands out pools connected to it.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const image = "postgres:17-alpine"

// Server is a PostgreSQL container shared by every test of a package.
type Server struct {
	container *postgres.PostgresContainer
	dsn       string
}

// Start boots the container. Callers are expected to keep it in a TestMain and to Stop it there,
// because starting one container per test would dominate the duration of the suite.
func Start(ctx context.Context) (*Server, error) {
	container, err := postgres.Run(ctx, image,
		postgres.WithDatabase("demo"),
		postgres.WithUsername("demo"),
		postgres.WithPassword("demo"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to build connection string: %w", err)
	}

	return &Server{container: container, dsn: dsn}, nil
}

// Stop removes the container.
func (s *Server) Stop(ctx context.Context) error {
	return testcontainers.TerminateContainer(s.container, testcontainers.StopContext(ctx))
}

// Pool opens a connection pool standing for one replica: two pools mean two independent processes,
// each with its own connections. Server messages such as the advisory lock warnings are collected
// in the returned Notices.
func (s *Server) Pool(t *testing.T) (*sql.DB, *Notices) {
	t.Helper()

	config, err := pgx.ParseConfig(s.dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn: %v", err)
	}

	notices := &Notices{}
	config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		notices.add(notice)
	}

	name := stdlib.RegisterConnConfig(config)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(name) })

	db, err := sql.Open("pgx", name)
	if err != nil {
		t.Fatalf("failed to open pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db, notices
}

// Notices collects the messages PostgreSQL sends to the client outside of query results.
type Notices struct {
	mu       sync.Mutex
	messages []string
}

func (n *Notices) add(notice *pgconn.Notice) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messages = append(n.messages, fmt.Sprintf("%s: %s", notice.Severity, notice.Message))
}

// All returns every message received so far, newest last.
func (n *Notices) All() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.messages...)
}
