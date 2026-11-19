package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"opps/internal/bulkmove"
	"opps/internal/platform"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Its own pool, sized Workers+1: bulk work can never take an API
	// connection. [isolation: separate pools]
	pool, err := platform.NewPool(ctx, dbURL, int32(cfg.Workers+1), "bulk-worker")
	if err != nil {
		return err
	}
	defer pool.Close()

	w, err := bulkmove.NewWorker(bulkmove.NewRepository(pool), cfg, log)
	if err != nil {
		return err
	}
	log.Info("worker started", "workers", cfg.Workers, "chunk_size", cfg.ChunkSize,
		"chunk_pause", cfg.ChunkPause, "lease_ttl", cfg.LeaseTTL, "lock_timeout", cfg.LockTimeout)
	w.Run(ctx)
	log.Info("worker stopped")
	return nil
}

func configFromEnv() (bulkmove.Config, error) {
	cfg := bulkmove.DefaultConfig()
	ints := map[string]*int{
		"WORKERS":            &cfg.Workers,
		"CHUNK_SIZE":         &cfg.ChunkSize,
		"MAX_CHUNK_FAILURES": &cfg.MaxChunkFailures,
	}
	for name, dst := range ints {
		if v := os.Getenv(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return cfg, fmt.Errorf("%s: %w", name, err)
			}
			*dst = n
		}
	}
	durations := map[string]*time.Duration{
		"CHUNK_PAUSE":   &cfg.ChunkPause,
		"LOCK_TIMEOUT":  &cfg.LockTimeout,
		"LEASE_TTL":     &cfg.LeaseTTL,
		"POLL_EVERY":    &cfg.PollEvery,
		"RETRY_BACKOFF": &cfg.RetryBackoff,
	}
	for name, dst := range durations {
		if v := os.Getenv(name); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return cfg, fmt.Errorf("%s: %w", name, err)
			}
			*dst = d
		}
	}
	return cfg, cfg.Validate()
}
