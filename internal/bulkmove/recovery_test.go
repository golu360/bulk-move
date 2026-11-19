package bulkmove

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/platform/testdb"
)

// ---------------------------------------------------------------------------
// 5. Resume and crash recovery
//
// Crash mid-chunk (scenario 26) is TestWorkerCrashMidChunkThenResume and
// a shutdown at an arbitrary point is TestWorkerShutdownThenRestart, both
// in worker_test.go.
// ---------------------------------------------------------------------------

// runCrashingWorker runs a one-goroutine worker that dies (runtime.Goexit,
// like a killed process: no commit, no lease release) inside its crashAt-th
// chunk, after the chunk's writes and before COMMIT. It returns once the
// worker is dead.
func runCrashingWorker(t *testing.T, pool *pgxpool.Pool, cfg Config, crashAt int) {
	t.Helper()
	repo := NewRepository(pool)
	calls := 0 // only the single worker goroutine touches it
	repo.beforeCommit = func() error {
		if calls++; calls == crashAt {
			runtime.Goexit()
		}
		return nil
	}
	cfg.Workers = 1
	done := make(chan struct{})
	go func() { defer close(done); newWorker(t, repo, cfg).Run(context.Background()) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("worker did not crash at chunk %d", crashAt)
	}
}

// countingRepo returns a repository that counts committed-or-attempted
// chunks (calls to the pre-COMMIT hook).
func countingRepo(pool *pgxpool.Pool) (*Repository, *atomic.Int32) {
	repo := NewRepository(pool)
	var n atomic.Int32
	repo.beforeCommit = func() error { n.Add(1); return nil }
	return repo, &n
}

// bulkTransitionsDigest fingerprints the job's transition rows up to and
// including maxID (id, deal, created_at): unchanged digest = not redone.
func bulkTransitionsDigest(t *testing.T, pool *pgxpool.Pool, jobID, maxID int64) string {
	t.Helper()
	var h string
	if err := pool.QueryRow(context.Background(), `
		SELECT coalesce(md5(string_agg(id || ':' || opportunity_id || ':' || created_at, ',' ORDER BY id)), '')
		FROM stage_transitions WHERE bulk_job_id = $1 AND id <= $2`, jobID, maxID).Scan(&h); err != nil {
		t.Fatal(err)
	}
	return h
}

// watchProgress samples the job's progress until stopped and fails the test
// if cursor, moved, skipped or moved+skipped ever go backwards. stop returns
// how many distinct cursor values were seen.
func watchProgress(t *testing.T, pool *pgxpool.Pool, jobID int64) (stop func() int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	distinct := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		last := [3]int{-1, -1, -1}
		for ctx.Err() == nil {
			var cur [3]int
			err := pool.QueryRow(ctx, `SELECT cursor_seq, moved, skipped FROM bulk_jobs WHERE id = $1`, jobID).
				Scan(&cur[0], &cur[1], &cur[2])
			if err != nil {
				if ctx.Err() == nil {
					t.Errorf("watch progress: %v", err)
				}
				return
			}
			if cur[0] < last[0] || cur[1] < last[1] || cur[2] < last[2] || cur[1]+cur[2] < last[1]+last[2] {
				t.Errorf("progress went backwards: cursor/moved/skipped %v -> %v", last, cur)
				return
			}
			if cur[0] != last[0] {
				distinct++
			}
			last = cur
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return func() int { cancel(); wg.Wait(); return distinct }
}

// 27. The worker dies between chunks: chunk 7 committed, chunk 8 never
// started, lease still held. A new worker resumes from the cursor after
// the lease expires and runs only the 13 remaining chunks; the 700
// transitions already committed are untouched (not redone).
func TestCrashBetweenChunksResumesFromCursor(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 2000) // 20 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()

	repoA := NewRepository(pool)
	a, err := repoA.Claim(ctx, "token-a", cfg.LeaseTTL)
	if err != nil || a == nil {
		t.Fatalf("claim = %+v, %v", a, err)
	}
	for range 7 {
		if _, err := repoA.ApplyChunk(ctx, a, cfg); err != nil {
			t.Fatal(err)
		}
	}
	// A is now gone: nothing in flight, lease held until it expires.
	j := loadJob(t, pool, jobID)
	if j.Cursor != 700 || j.Moved != 700 || j.Status != "running" || j.LeaseToken == nil {
		t.Fatalf("after crash: %+v, want running cursor=moved=700 with a lease", j)
	}
	maxID := int64(count(t, pool, `SELECT max(id) FROM stage_transitions WHERE bulk_job_id = $1`, jobID))
	before := bulkTransitionsDigest(t, pool, jobID, maxID)

	repoB, chunksB := countingRepo(pool)
	startWorker(t, newWorker(t, repoB, cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))

	j = assertCompleted(t, pool, jobID)
	if j.Moved != 2000 || j.Skipped != 0 || j.Claims != 2 {
		t.Fatalf("moved=%d skipped=%d claims=%d, want 2000/0/2", j.Moved, j.Skipped, j.Claims)
	}
	if got := chunksB.Load(); got != 13 {
		t.Fatalf("worker B ran %d chunks, want 13 (only those after the cursor)", got)
	}
	if after := bulkTransitionsDigest(t, pool, jobID, maxID); after != before {
		t.Fatal("transitions committed before the crash were rewritten")
	}
}

// 28. The worker dies right after claiming, before its first chunk. The
// job sits "running" with a lease and no progress; after the lease expires
// another worker claims it and runs it from the start.
func TestCrashAfterClaimBeforeFirstChunk(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()

	if a, err := NewRepository(pool).Claim(ctx, "token-a", cfg.LeaseTTL); err != nil || a == nil {
		t.Fatalf("claim = %+v, %v", a, err)
	}
	if j := loadJob(t, pool, jobID); j.Status != "running" || j.Cursor != 0 || j.Claims != 1 || j.StartedAt == nil {
		t.Fatalf("after claim: %+v", j)
	}

	repoB, chunksB := countingRepo(pool)
	startWorker(t, newWorker(t, repoB, cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))

	j := assertCompleted(t, pool, jobID)
	if j.Moved != 1000 || j.Claims != 2 || chunksB.Load() != 10 {
		t.Fatalf("moved=%d claims=%d chunks=%d, want 1000/2/10", j.Moved, j.Claims, chunksB.Load())
	}
}

// 29. The end of the job. Completion is written in the SAME transaction as
// the last chunk, so "last chunk committed but not completed" cannot arise
// from a crash in this design; the cases are:
//   - crash inside the last chunk before COMMIT: the chunk is redone once;
//   - the job is at cursor = total but still running (forced, e.g. by an
//     older build or a manual fix): resume only marks it completed;
//   - an empty job (total 0) crashed after claim: resume completes it.
//
// In every case no deal is moved twice.
func TestCrashAtEndOfJob(t *testing.T) {
	t.Run("crash inside the last chunk", func(t *testing.T) {
		pool := testdb.New(t)
		ctx := context.Background()
		ws, s1, s2 := seedWorkspace(t, pool, "A")
		insertOpps(t, pool, ws, s1, 1000) // 10 chunks
		jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
		cfg := testConfig()

		repoA := NewRepository(pool)
		a, err := repoA.Claim(ctx, "token-a", cfg.LeaseTTL)
		if err != nil || a == nil {
			t.Fatalf("claim = %+v, %v", a, err)
		}
		for range 9 {
			if _, err := repoA.ApplyChunk(ctx, a, cfg); err != nil {
				t.Fatal(err)
			}
		}
		// The last chunk does all its writes, then dies before COMMIT.
		crash := errors.New("crash before commit")
		repoA.beforeCommit = func() error { return crash }
		if done, err := repoA.ApplyChunk(ctx, a, cfg); !errors.Is(err, crash) || done {
			t.Fatalf("last chunk = %v, %v; want the crash", done, err)
		}
		if j := loadJob(t, pool, jobID); j.Status != "running" || j.Cursor != 900 || j.Moved != 900 || j.FinishedAt != nil {
			t.Fatalf("after crash: %+v, want running at cursor 900", j)
		}

		repoB, chunksB := countingRepo(pool)
		startWorker(t, newWorker(t, repoB, cfg))
		waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
		j := assertCompleted(t, pool, jobID)
		if j.Moved != 1000 || j.Claims != 2 || chunksB.Load() != 1 {
			t.Fatalf("moved=%d claims=%d chunks=%d, want 1000/2/1", j.Moved, j.Claims, chunksB.Load())
		}
	})

	t.Run("all chunks committed, completion not recorded", func(t *testing.T) {
		pool := testdb.New(t)
		ctx := context.Background()
		ws, s1, s2 := seedWorkspace(t, pool, "A")
		insertOpps(t, pool, ws, s1, 500)
		jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
		cfg := testConfig()

		repo := NewRepository(pool)
		a, err := repo.Claim(ctx, "token-a", cfg.LeaseTTL)
		if err != nil || a == nil {
			t.Fatalf("claim = %+v, %v", a, err)
		}
		for done := false; !done; {
			if done, err = repo.ApplyChunk(ctx, a, cfg); err != nil {
				t.Fatal(err)
			}
		}
		// Rewind only the completion: all work is done, status says running,
		// and the dead worker's lease has expired.
		if _, err := pool.Exec(ctx, `
			UPDATE bulk_jobs SET status = 'running', finished_at = NULL,
			       lease_token = 'dead', lease_expires_at = now() - interval '1 second'
			WHERE id = $1`, jobID); err != nil {
			t.Fatal(err)
		}
		movedAt := count(t, pool, `SELECT count(*) FROM stage_transitions WHERE bulk_job_id = $1`, jobID)

		repoB, chunksB := countingRepo(pool)
		startWorker(t, newWorker(t, repoB, cfg))
		waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
		j := assertCompleted(t, pool, jobID)
		if j.Moved != 500 || j.Claims != 2 || movedAt != 500 || chunksB.Load() != 0 {
			t.Fatalf("moved=%d claims=%d chunks=%d, want 500/2/0 (no extra moves)", j.Moved, j.Claims, chunksB.Load())
		}
	})

	t.Run("empty job crashed after claim", func(t *testing.T) {
		pool := testdb.New(t)
		ws, s1, s2 := seedWorkspace(t, pool, "A")
		jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2) // matches nothing
		cfg := testConfig()
		if a, err := NewRepository(pool).Claim(context.Background(), "token-a", cfg.LeaseTTL); err != nil || a == nil {
			t.Fatalf("claim = %+v, %v", a, err)
		}
		startWorker(t, newWorker(t, NewRepository(pool), cfg))
		waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
		if j := assertCompleted(t, pool, jobID); j.Total != 0 || j.Moved != 0 || j.Claims != 2 {
			t.Fatalf("job = %+v", j)
		}
	})
}

// 30. Graceful shutdown (SIGTERM cancels the worker's context) while a
// chunk is in flight: that chunk rolls back, the lease is kept (not
// released), and a restarted worker redoes exactly that chunk and finishes.
// Deterministic: the hook parks chunk 10 until the context is cancelled.
func TestGracefulShutdownMidChunk(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 3000) // 30 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()

	repoA := NewRepository(pool)
	inChunk, release := make(chan struct{}), make(chan struct{})
	calls := 0
	repoA.beforeCommit = func() error {
		if calls++; calls == 10 {
			close(inChunk)
			<-release
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); newWorker(t, repoA, cfg).Run(ctx) }()

	select {
	case <-inChunk:
	case <-time.After(30 * time.Second):
		t.Fatal("chunk 10 never started")
	}
	cancel() // SIGTERM: signal.NotifyContext cancels this context in cmd/worker
	close(release)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not stop after shutdown")
	}

	j := loadJob(t, pool, jobID)
	if j.Status != "running" || j.Cursor != 900 || j.Moved != 900 || j.LeaseToken == nil || j.LastError != nil {
		t.Fatalf("after shutdown: %+v, want running, cursor=moved=900 (chunk 10 rolled back), lease kept, no error", j)
	}
	assertTransitionsMatch(t, pool, jobID, j)
	if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE stage_id = $1`, s2); n != 900 {
		t.Fatalf("%d deals in the target, want 900: the in-flight chunk leaked", n)
	}

	repoB, chunksB := countingRepo(pool)
	startWorker(t, newWorker(t, repoB, cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	j = assertCompleted(t, pool, jobID)
	if j.Moved != 3000 || j.Claims != 2 || chunksB.Load() != 21 {
		t.Fatalf("moved=%d claims=%d chunks=%d, want 3000/2/21", j.Moved, j.Claims, chunksB.Load())
	}
}

// 31. Crash, make progress, crash again: each worker commits 5 chunks and
// dies inside its 6th, four times over; the fifth worker finishes. Crashes
// are not chunk failures: the job completes, is never marked failed, and
// keeps no error.
func TestRepeatedCrashesComplete(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 3000) // 30 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.MaxChunkFailures = 1 // a crash counted as a failure would fail the job at once

	stopWatch := watchProgress(t, pool, jobID)
	for crash := 1; crash <= 4; crash++ {
		runCrashingWorker(t, pool, cfg, 6)
		j := loadJob(t, pool, jobID)
		if j.Status != "running" || j.Cursor != crash*500 || j.Claims != crash || j.LastError != nil {
			t.Fatalf("after crash %d: %+v, want running at cursor %d, claims %d", crash, j, crash*500, crash)
		}
		assertTransitionsMatch(t, pool, jobID, j)
	}
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	stopWatch()

	j := assertCompleted(t, pool, jobID)
	if j.Moved != 3000 || j.Claims != 5 || j.LastError != nil {
		t.Fatalf("moved=%d claims=%d last_error=%v, want 3000/5/nil", j.Moved, j.Claims, j.LastError)
	}
}

// 33. Progress never goes backwards across every kind of interruption in
// one job's life: a crash mid-chunk, a takeover, a graceful shutdown
// mid-job, a crash between chunks, and the final resume.
func TestProgressIsMonotonicAcrossCrashes(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 4000) // 40 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.ChunkPause = 5 * time.Millisecond // let the watcher see intermediate states

	stopWatch := watchProgress(t, pool, jobID)

	// 1. Crash inside chunk 8.
	runCrashingWorker(t, pool, cfg, 8)

	// 2. A new worker takes over, then is shut down gracefully mid-job.
	stop := startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "progress past 2000", func() bool { return loadJob(t, pool, jobID).Cursor >= 2000 })
	stop()

	// 3. Another claims (after the lease expires), commits 3 chunks, dies
	// between chunks.
	repo := NewRepository(pool)
	var c *ClaimedJob
	waitFor(t, 10*time.Second, "job claimable", func() bool {
		var err error
		c, err = repo.Claim(ctx, "token-c", cfg.LeaseTTL)
		if err != nil {
			t.Fatal(err)
		}
		return c != nil
	})
	for range 3 {
		if _, err := repo.ApplyChunk(ctx, c, cfg); err != nil {
			t.Fatal(err)
		}
	}

	// 4. The last worker finishes.
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	seen := stopWatch()

	j := assertCompleted(t, pool, jobID)
	if j.Moved != 4000 || j.Claims != 4 {
		t.Fatalf("moved=%d claims=%d, want 4000/4", j.Moved, j.Claims)
	}
	if seen < 10 {
		t.Fatalf("watcher saw only %d distinct cursor values; it did not observe the job", seen)
	}
	t.Logf("watcher saw %d distinct cursor values, none going backwards", seen)
}
