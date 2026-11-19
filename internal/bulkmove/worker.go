package bulkmove

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Config controls the worker. Isolation knobs are called out explicitly.
type Config struct {
	// Workers is the number of claim loops in this process. With its own
	// pool of Workers+1 connections (cmd/worker), bulk work can never take
	// an API connection. [isolation: separate pools]
	Workers int
	// ChunkSize and ChunkPause pace the job: short transactions keep row
	// locks to milliseconds, and the pause leaves room for other traffic.
	// [isolation: chunk pacing]
	ChunkSize  int
	ChunkPause time.Duration
	// LockTimeout bounds how long a chunk waits on a user's row lock before
	// it rolls back and is retried. [isolation: bounded lock waits]
	LockTimeout time.Duration
	// LeaseTTL: a job with no committed chunk for this long is considered
	// abandoned and may be claimed by any worker. Committing a chunk renews
	// it; there is no heartbeat.
	LeaseTTL time.Duration
	// PollEvery is the sleep when nothing is claimable.
	PollEvery time.Duration
	// MaxChunkFailures failed attempts at the same chunk mark the job
	// failed. An attempt fails when the chunk returns an error, or when the
	// worker dies holding the lease (counted when another worker takes the
	// job over). Committed progress resets the count.
	MaxChunkFailures int
	// RetryBackoff: after a failed chunk the worker releases the job, and
	// no worker may claim it again for this long.
	RetryBackoff time.Duration
}

func DefaultConfig() Config {
	return Config{
		Workers:          4,
		ChunkSize:        500,
		ChunkPause:       50 * time.Millisecond,
		LockTimeout:      5 * time.Second,
		LeaseTTL:         30 * time.Second,
		PollEvery:        time.Second,
		MaxChunkFailures: 5,
		RetryBackoff:     time.Second,
	}
}

func (c Config) Validate() error {
	switch {
	case c.Workers < 1:
		return errors.New("workers must be >= 1")
	case c.ChunkSize < 1:
		return errors.New("chunk size must be >= 1")
	case c.MaxChunkFailures < 1:
		return errors.New("max chunk failures must be >= 1")
	case c.LockTimeout <= 0 || c.LeaseTTL <= 0 || c.PollEvery <= 0:
		return errors.New("lock timeout, lease TTL and poll interval must be > 0")
	case c.LeaseTTL <= c.LockTimeout+c.ChunkPause:
		// Otherwise a chunk legitimately waiting on a lock could lose its
		// lease mid-flight and the job would bounce between workers.
		return fmt.Errorf("lease TTL (%v) must exceed lock timeout + chunk pause (%v)",
			c.LeaseTTL, c.LockTimeout+c.ChunkPause)
	}
	return nil
}

// Worker runs bulk move jobs: a fixed number of goroutines, each looping
// claim → run job to the end → claim again.
type Worker struct {
	repo *Repository
	cfg  Config
	log  *slog.Logger
}

func NewWorker(repo *Repository, cfg Config, log *slog.Logger) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("worker config: %w", err)
	}
	return &Worker{repo: repo, cfg: cfg, log: log}, nil
}

// Run blocks until ctx is cancelled and every loop has returned. On
// cancellation an in-flight chunk rolls back; its lease is not released, so
// another worker resumes the job after LeaseTTL.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range w.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx, w.log.With("slot", i))
		}()
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context, log *slog.Logger) {
	for ctx.Err() == nil {
		token, err := newLeaseToken()
		if err != nil {
			log.Error("mint lease token", "error", err)
			sleep(ctx, w.cfg.PollEvery)
			continue
		}
		job, err := w.repo.Claim(ctx, token, w.cfg.LeaseTTL)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("claim", "error", err)
			}
			sleep(ctx, w.cfg.PollEvery)
			continue
		}
		if job == nil {
			sleep(ctx, w.cfg.PollEvery)
			continue
		}
		w.runJob(ctx, job, log.With("job_id", job.ID, "workspace_id", job.WorkspaceID))
	}
}

// runJob applies chunks until the job completes, the lease is lost, the
// context ends, or a chunk fails. A failed chunk is not retried here: the
// job is released with a backoff (or failed, at MaxChunkFailures) and any
// worker, this one included, may claim it again once the backoff is over.
func (w *Worker) runJob(ctx context.Context, job *ClaimedJob, log *slog.Logger) {
	log.Info("job claimed", "attempts", job.Attempts, "cursor", job.Cursor)
	if job.Attempts >= w.cfg.MaxChunkFailures {
		// Poison job: every worker that ran this chunk died holding the lease.
		reason := fmt.Sprintf("abandoned %d times at item %d without progress (worker died or lost its lease each time)",
			job.Attempts, job.Cursor)
		if ok, err := w.repo.MarkFailed(ctx, job, reason); err != nil {
			log.Error("mark failed", "error", err)
		} else if ok {
			log.Error("job failed", "reason", reason)
		}
		return
	}
	for {
		done, err := w.repo.ApplyChunk(ctx, job, w.cfg)
		switch {
		case err == nil && done:
			log.Info("job completed")
			return
		case err == nil:
			if !sleep(ctx, w.cfg.ChunkPause) {
				return
			}
		case errors.Is(err, ErrLeaseLost):
			log.Warn("lease lost; another worker owns the job")
			return
		case ctx.Err() != nil:
			log.Info("shutting down; in-flight chunk rolled back, job resumes after lease expiry")
			return
		default:
			status, ferr := w.repo.ReleaseAfterFailure(ctx, job, err.Error(), w.cfg.RetryBackoff, w.cfg.MaxChunkFailures)
			switch {
			case ferr != nil:
				log.Error("release after failure", "error", ferr, "chunk_error", err)
			case status == "failed":
				log.Error("job failed", "reason", err, "attempts", w.cfg.MaxChunkFailures)
			case status != "":
				log.Warn("chunk failed; job released for retry", "error", err, "backoff", w.cfg.RetryBackoff)
			}
			return
		}
	}
}

// sleep waits d or until ctx ends; it reports whether the full d elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
