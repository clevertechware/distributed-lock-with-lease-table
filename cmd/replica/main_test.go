package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestEveryDurationFlagMustBePositive(t *testing.T) {
	broken := []struct {
		flag string
		cfg  config
	}{
		{"lease", config{lease: 0, interval: time.Second, work: time.Second}},
		{"interval", config{lease: time.Second, interval: 0, work: time.Second}},
		{"work", config{lease: time.Second, interval: time.Second, work: -time.Second}},
	}

	for _, candidate := range broken {
		assert.ErrorContains(t, validate(candidate.cfg), "-"+candidate.flag+" must be positive")
	}
}

func TestTheDurationsOfTheDemonstrationAreAccepted(t *testing.T) {
	assert.NoError(t, validate(config{lease: 6 * time.Second, interval: 2 * time.Second, work: 5 * time.Second}))
}

func TestTheDefaultNameSetsTwoReplicasOfTheSameMachineApart(t *testing.T) {
	host, err := os.Hostname()
	require.NoError(t, err)

	assert.Equal(t, fmt.Sprintf("%s-%d", host, os.Getpid()), defaultName())
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
