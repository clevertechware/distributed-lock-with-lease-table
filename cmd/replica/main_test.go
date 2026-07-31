package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBothImplementationsOfTheArticleAreSelectable(t *testing.T) {
	for _, name := range []string{"lease", "advisory"} {
		selected, err := parseMode(name)

		assert.NoError(t, err)
		assert.Equal(t, mode(name), selected)
	}
}

func TestAnUnknownLockerIsRejectedBeforeTheReplicaConnects(t *testing.T) {
	_, err := parseMode("redis")

	assert.ErrorContains(t, err, `unknown locker "redis"`)
}

func TestWorkStopsAsSoonAsTheReplicaIsShutDown(t *testing.T) {
	ctx, shutdown := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- work(time.Hour)(ctx) }()
	shutdown()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("the job kept running after the shutdown, the lease would be left to expire")
	}
}

func TestDSNFallsBackOnTheThrowawayDatabaseWhenTheEnvironmentIsSilent(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	assert.Equal(t, defaultDSN, dsn())
}

func TestDSNPrefersTheEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://elsewhere/db")

	assert.Equal(t, "postgres://elsewhere/db", dsn())
}
