package bulkmove

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/platform/testdb"
	"opps/internal/stage"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newDB returns a fresh migrated database for the test and registers the
// invariant check (checkInvariants) to run when the test ends: after the
// test's workers have stopped (their cleanups were registered later, so
// they run first) and before the database is dropped. Every bulk test gets
// it without asking; tests that corrupt data on purpose opt out with
// skipInvariants.
func newDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t)
	t.Cleanup(func() {
		if t.Failed() || t.Skipped() {
			return
		}
		if _, skip := invariantOptOuts.Load(t); skip {
			invariantOptOuts.Delete(t)
			return
		}
		checkInvariants(t, pool)
	})
	return pool
}

var invariantOptOuts sync.Map // testing.TB → struct{}

// skipInvariants turns off the end-of-test invariant check for a test that
// deliberately writes inconsistent data.
func skipInvariants(t testing.TB) { invariantOptOuts.Store(t, struct{}{}) }

// checkInvariants holds for every job in the database, whatever its status
// (pending, mid-run, failed or completed), because progress, moves and
// transitions only ever commit together:
//   - moved + skipped = processed (cursor_seq), and processed <= total
//   - bulk transitions for the job = moved
//   - no deal has two bulk transitions for the same job
//   - nothing outside the job's workspace was touched: every bulk
//     transition is recorded in, and moves a deal of, the job's workspace,
//     and is for a deal in the job's snapshot
func checkInvariants(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	checks := []struct{ name, sql string }{
		{"moved + skipped = processed <= total", `
			SELECT count(*) FROM bulk_jobs
			WHERE moved + skipped <> cursor_seq OR cursor_seq > total`},
		{"bulk transitions = moved", `
			SELECT count(*) FROM bulk_jobs j
			WHERE j.moved <> (SELECT count(*) FROM stage_transitions s WHERE s.bulk_job_id = j.id)`},
		{"no deal with two bulk transitions for one job", `
			SELECT count(*) FROM (SELECT 1 FROM stage_transitions WHERE bulk_job_id IS NOT NULL
			                      GROUP BY bulk_job_id, opportunity_id HAVING count(*) > 1) d`},
		{"no bulk transition outside the job's workspace", `
			SELECT count(*) FROM stage_transitions s
			JOIN bulk_jobs j ON j.id = s.bulk_job_id
			JOIN opportunities o ON o.id = s.opportunity_id
			WHERE s.workspace_id <> j.workspace_id OR o.workspace_id <> j.workspace_id`},
		{"no bulk transition for a deal outside the snapshot", `
			SELECT count(*) FROM stage_transitions s
			WHERE s.bulk_job_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM bulk_job_items i
				WHERE i.job_id = s.bulk_job_id AND i.opportunity_id = s.opportunity_id)`},
		{"no bulk transition without its job", `
			SELECT count(*) FROM stage_transitions s
			WHERE s.bulk_job_id IS NOT NULL
			  AND NOT EXISTS (SELECT 1 FROM bulk_jobs j WHERE j.id = s.bulk_job_id)`},
	}
	for _, c := range checks {
		if n := count(t, pool, c.sql); n != 0 {
			t.Errorf("invariant violated after test: %s (%d rows)", c.name, n)
		}
	}
}

// testConfig is fast: 1s lease, short timeouts, no pacing.
func testConfig() Config {
	return Config{
		Workers:          1,
		ChunkSize:        100,
		ChunkPause:       0,
		LockTimeout:      300 * time.Millisecond,
		LeaseTTL:         time.Second,
		PollEvery:        20 * time.Millisecond,
		MaxChunkFailures: 5,
		RetryBackoff:     10 * time.Millisecond,
	}
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newWorker(t testing.TB, repo *Repository, cfg Config) *Worker {
	t.Helper()
	w, err := NewWorker(repo, cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// startWorker runs w in the background; the returned stop cancels it and
// waits for Run to return. stop is also registered as cleanup.
func startWorker(t testing.TB, w *Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("worker did not stop within 15s")
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type jobState struct {
	Status                                string
	Total, Cursor, Moved, Skipped, Claims int
	LeaseToken, LastError                 *string
	StartedAt, FinishedAt                 *time.Time
	TargetStageID                         int64
	Attempts                              int
}

func loadJob(t testing.TB, pool *pgxpool.Pool, id int64) jobState {
	t.Helper()
	var j jobState
	if err := pool.QueryRow(context.Background(), `
		SELECT status, total, cursor_seq, moved, skipped, claims, lease_token, last_error,
		       started_at, finished_at, target_stage_id, attempts
		FROM bulk_jobs WHERE id = $1`, id).Scan(
		&j.Status, &j.Total, &j.Cursor, &j.Moved, &j.Skipped, &j.Claims, &j.LeaseToken,
		&j.LastError, &j.StartedAt, &j.FinishedAt, &j.TargetStageID, &j.Attempts); err != nil {
		t.Fatal(err)
	}
	return j
}

func statusIs(t testing.TB, pool *pgxpool.Pool, id int64, status string) func() bool {
	return func() bool { return loadJob(t, pool, id).Status == status }
}

func submitJob(t testing.TB, pool *pgxpool.Pool, ws int64, key string, f Filter, target int64) int64 {
	t.Helper()
	job, _, err := newService(pool).Submit(context.Background(), ws, key, Request{Filter: f, TargetStageID: target})
	if err != nil {
		t.Fatal(err)
	}
	return job.ID
}

func bulkTransitions(t testing.TB, pool *pgxpool.Pool, jobID int64) int {
	return count(t, pool, `SELECT count(*) FROM stage_transitions WHERE bulk_job_id = $1`, jobID)
}

// assertCompleted checks the job finished correctly: completed, every item
// accounted for, one transition per moved deal, no deal moved twice, and
// each moved deal's history agrees with its state.
func assertCompleted(t testing.TB, pool *pgxpool.Pool, jobID int64) jobState {
	t.Helper()
	j := loadJob(t, pool, jobID)
	if j.Status != "completed" || j.FinishedAt == nil || j.LeaseToken != nil {
		t.Fatalf("job %d: status=%s finished_at=%v lease_token=%v, want completed/set/nil",
			jobID, j.Status, j.FinishedAt, j.LeaseToken)
	}
	if j.Cursor != j.Total || j.Moved+j.Skipped != j.Total {
		t.Fatalf("job %d: cursor=%d moved=%d skipped=%d total=%d", jobID, j.Cursor, j.Moved, j.Skipped, j.Total)
	}
	assertTransitionsMatch(t, pool, jobID, j)
	return j
}

// assertTransitionsMatch holds at every committed point, mid-job too.
func assertTransitionsMatch(t testing.TB, pool *pgxpool.Pool, jobID int64, j jobState) {
	t.Helper()
	if n := bulkTransitions(t, pool, jobID); n != j.Moved {
		t.Fatalf("job %d: %d bulk transitions, moved=%d", jobID, n, j.Moved)
	}
	if n := count(t, pool, `
		SELECT count(*) FROM (SELECT opportunity_id FROM stage_transitions
		WHERE bulk_job_id = $1 GROUP BY 1 HAVING count(*) > 1) d`, jobID); n != 0 {
		t.Fatalf("job %d: %d deals with more than one bulk transition", jobID, n)
	}
	// Every deal the job moved, and that nobody touched since, is in the
	// target stage.
	if n := count(t, pool, `
		SELECT count(*) FROM stage_transitions b
		JOIN opportunities o ON o.id = b.opportunity_id
		WHERE b.bulk_job_id = $1 AND o.stage_id <> $2
		  AND NOT EXISTS (SELECT 1 FROM stage_transitions l
		                  WHERE l.opportunity_id = b.opportunity_id AND l.id > b.id)`,
		jobID, j.TargetStageID); n != 0 {
		t.Fatalf("job %d: %d moved deals not in the target stage", jobID, n)
	}
}

// manualMove mirrors opportunity.MoveOpportunity: lock, bump version,
// record a 'manual' transition, in one transaction.
func manualMove(ctx context.Context, pool *pgxpool.Pool, ws, id, to int64) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var from int64
		if err := tx.QueryRow(ctx,
			`SELECT stage_id FROM opportunities WHERE id = $1 AND workspace_id = $2 FOR UPDATE`,
			id, ws).Scan(&from); err != nil {
			return err
		}
		if from == to {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`UPDATE opportunities SET stage_id = $2, version = version + 1, updated_at = now() WHERE id = $1`,
			id, to); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO stage_transitions (workspace_id, opportunity_id, from_stage_id, to_stage_id, source)
			VALUES ($1, $2, $3, $4, 'manual')`, ws, id, from, to)
		return err
	})
}

func oppIDs(t testing.TB, pool *pgxpool.Pool, stageID int64) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id FROM opportunities WHERE stage_id = $1 ORDER BY id`, stageID)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func addStage(t testing.TB, pool *pgxpool.Pool, ws int64, pos int) int64 {
	t.Helper()
	id, err := stage.Create(context.Background(), pool, ws, pos, "Extra")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// ---------------------------------------------------------------------------
// 1. Worker crashes and restarts
// ---------------------------------------------------------------------------

// Worker A dies inside chunk N after all its writes but before COMMIT
// (runtime.Goexit in the hook: the transaction never commits, the lease is
// never released). Worker B must resume after the lease expires, redo chunk
// N, and finish with every deal moved exactly once.
//
// Fails if: the chunk is not one transaction (partial writes survive the
// crash → duplicate or missing transitions), the cursor is not committed
// with the chunk (B redoes or skips work), or expired leases aren't
// reclaimable (B never starts).
func TestWorkerCrashMidChunkThenResume(t *testing.T) {
	for _, tc := range []struct {
		name    string
		crashAt int // 1-based chunk number; 5000 deals / 100 = 50 chunks
	}{
		{"first chunk", 1},
		{"middle chunk", 30},
		{"last chunk", 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := newDB(t)
			ws, s1, s2 := seedWorkspace(t, pool, "A")
			insertOpps(t, pool, ws, s1, 5000)
			jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
			cfg := testConfig()

			// Worker A: crashes in chunk crashAt.
			repoA := NewRepository(pool)
			calls := 0 // only the single worker goroutine touches it
			repoA.beforeCommit = func() error {
				calls++
				if calls == tc.crashAt {
					runtime.Goexit()
				}
				return nil
			}
			aDone := make(chan struct{})
			go func() { defer close(aDone); newWorker(t, repoA, cfg).Run(context.Background()) }()
			select {
			case <-aDone:
			case <-time.After(30 * time.Second):
				t.Fatal("worker A did not crash")
			}

			// After the crash: exactly the committed chunks are visible.
			want := (tc.crashAt - 1) * cfg.ChunkSize
			j := loadJob(t, pool, jobID)
			if j.Status != "running" || j.Cursor != want || j.Moved != want || j.Claims != 1 || j.LeaseToken == nil {
				t.Fatalf("after crash: %+v, want running cursor=moved=%d claims=1 with a lease", j, want)
			}
			assertTransitionsMatch(t, pool, jobID, j)
			if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE stage_id = $1`, s2); n != want {
				t.Fatalf("deals in target after crash = %d, want %d (uncommitted chunk leaked)", n, want)
			}

			// Worker B: a fresh process. Resumes once A's lease expires, and
			// not before: while the DB clock is before A's expiry, claims
			// must stay 1 (read in the same statement, so no race).
			var aExpires time.Time
			if err := pool.QueryRow(context.Background(),
				`SELECT lease_expires_at FROM bulk_jobs WHERE id = $1`, jobID).Scan(&aExpires); err != nil {
				t.Fatal(err)
			}
			stop := startWorker(t, newWorker(t, NewRepository(pool), cfg))
			for {
				var claims int
				var leased bool
				if err := pool.QueryRow(context.Background(),
					`SELECT claims, now() < $2 FROM bulk_jobs WHERE id = $1`, jobID, aExpires).Scan(&claims, &leased); err != nil {
					t.Fatal(err)
				}
				if !leased {
					break
				}
				if claims != 1 {
					t.Fatalf("worker B claimed the job while A's lease was still valid (claims=%d)", claims)
				}
				time.Sleep(10 * time.Millisecond)
			}
			waitFor(t, 30*time.Second, "job completed by worker B", statusIs(t, pool, jobID, "completed"))
			stop()

			j = assertCompleted(t, pool, jobID)
			if j.Moved != 5000 || j.Skipped != 0 || j.Claims != 2 {
				t.Fatalf("after resume: moved=%d skipped=%d claims=%d, want 5000/0/2", j.Moved, j.Skipped, j.Claims)
			}
			if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE stage_id = $1`, s2); n != 5000 {
				t.Fatalf("deals in target = %d, want 5000", n)
			}
		})
	}
}

// SIGTERM path: the context is cancelled mid-job. Whatever chunk was in
// flight either committed or rolled back; a restarted worker finishes the
// job with nothing lost or doubled.
func TestWorkerShutdownThenRestart(t *testing.T) {
	pool := newDB(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 3000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.ChunkPause = 5 * time.Millisecond

	stop := startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "some progress", func() bool { return loadJob(t, pool, jobID).Cursor >= 1000 })
	stop()

	j := loadJob(t, pool, jobID)
	if j.Status != "running" || j.Cursor >= j.Total {
		t.Fatalf("after shutdown: %+v, want running and unfinished", j)
	}
	assertTransitionsMatch(t, pool, jobID, j)

	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "job completed after restart", statusIs(t, pool, jobID, "completed"))
	j = assertCompleted(t, pool, jobID)
	if j.Moved != 3000 || j.Claims != 2 {
		t.Fatalf("moved=%d claims=%d, want 3000/2", j.Moved, j.Claims)
	}
}

// ---------------------------------------------------------------------------
// 2. Retrying a chunk
// ---------------------------------------------------------------------------

// 38. Transient failures: chunk 3 fails its first 2 attempts, and so
// does chunk 6. Each failure rolls back, releases the job, and the retry
// succeeds with nothing doubled. The limit is 3 failed attempts per chunk:
// 4 failures in all would exceed it, so the job completes only because
// chunk 3's success reset the count (attempts at chunk 6's retry is 2,
// not 4).
func TestChunkRetryAfterTransientFailure(t *testing.T) {
	pool := newDB(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000) // 10 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.MaxChunkFailures = 3

	var (
		mu       sync.Mutex
		calls    int
		attempts = map[int]int{} // call number → committed attempts when it ran
	)
	repo := NewRepository(pool)
	repo.beforeCommit = func() error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		var a int
		if err := pool.QueryRow(context.Background(), `SELECT attempts FROM bulk_jobs WHERE id = $1`, jobID).Scan(&a); err != nil {
			return err
		}
		attempts[calls] = a
		// Calls 3, 4, 5 are chunk 3; calls 8, 9, 10 are chunk 6.
		if calls == 3 || calls == 4 || calls == 8 || calls == 9 {
			return errors.New("injected transient failure")
		}
		return nil
	}
	startWorker(t, newWorker(t, repo, cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))

	j := assertCompleted(t, pool, jobID)
	if j.Moved != 1000 || j.Skipped != 0 || j.LastError != nil || j.Attempts != 0 {
		t.Fatalf("job = %+v, want moved=1000 skipped=0, attempts and last_error cleared", j)
	}
	if j.Claims != 5 {
		t.Fatalf("claims = %d, want 5 (first claim + one per failure)", j.Claims)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 14 {
		t.Fatalf("chunk attempts = %d, want 14 (10 chunks + 4 retries)", calls)
	}
	for call, want := range map[int]int{3: 0, 4: 1, 5: 2, 6: 0, 8: 0, 9: 1, 10: 2} {
		if attempts[call] != want {
			t.Fatalf("attempts seen by call %d = %d, want %d (all: %v)", call, attempts[call], want, attempts)
		}
	}
}

// 39. A chunk that keeps failing marks the job failed after
// MaxChunkFailures attempts, with last_error set; it keeps the committed
// progress, releases the lease, and is never claimed again. The workspace
// is then free for its next job.
func TestChunkRetryExhaustedMarksJobFailed(t *testing.T) {
	pool := newDB(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 1000)
	insertOpps(t, pool, ws, s3, 100)
	jobID := submitJob(t, pool, ws, "k1", Filter{StageID: &s1}, s2)

	cfg := testConfig()
	cfg.MaxChunkFailures = 3
	repo := NewRepository(pool)
	var calls atomic.Int32
	repo.beforeCommit = func() error {
		if calls.Add(1) >= 3 { // chunk 3 onwards always fails
			return errors.New("injected persistent failure")
		}
		return nil
	}
	stop := startWorker(t, newWorker(t, repo, cfg))
	waitFor(t, 30*time.Second, "job failed", statusIs(t, pool, jobID, "failed"))
	time.Sleep(3 * cfg.PollEvery) // give the worker a chance to (wrongly) reclaim it
	stop()

	j := loadJob(t, pool, jobID)
	if j.Cursor != 200 || j.Moved != 200 || j.LeaseToken != nil || j.FinishedAt == nil || j.Claims != 3 || j.Attempts != 3 {
		t.Fatalf("failed job = %+v, want cursor=moved=200, no lease, finished, claims=3, attempts=3", j)
	}
	if j.LastError == nil || !strings.Contains(*j.LastError, "injected persistent failure") {
		t.Fatalf("last_error = %v", j.LastError)
	}
	if got := calls.Load(); got != 2+3 {
		t.Fatalf("attempts = %d, want 5 (2 good chunks + 3 failures)", got)
	}
	assertTransitionsMatch(t, pool, jobID, j)

	// A failed job does not hold the workspace: the next job runs.
	job2 := submitJob(t, pool, ws, "k2", Filter{StageID: &s3}, s2)
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "second job completed", statusIs(t, pool, job2, "completed"))
	assertCompleted(t, pool, job2)
	if j := loadJob(t, pool, jobID); j.Status != "failed" || j.Claims != 3 {
		t.Fatalf("failed job was reclaimed: %+v", j)
	}
}

// A real retry cause: a user holds a row lock longer than LockTimeout. The
// chunk times out and rolls back, is retried, and once the user commits
// (bumping version) the deal is skipped and the rest move.
func TestChunkRetryOnLockTimeout(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 1000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	first := oppIDs(t, pool, s1)[0] // in chunk 1

	userTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer userTx.Rollback(ctx)
	if _, err := userTx.Exec(ctx,
		`UPDATE opportunities SET stage_id = $2, version = version + 1 WHERE id = $1`, first, s3); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.LockTimeout = 200 * time.Millisecond
	cfg.RetryBackoff = 50 * time.Millisecond
	cfg.MaxChunkFailures = 50
	cfg.LeaseTTL = 5 * time.Second
	startWorker(t, newWorker(t, NewRepository(pool), cfg))

	time.Sleep(time.Second) // ~4 lock timeouts on chunk 1
	if j := loadJob(t, pool, jobID); j.Cursor != 0 || j.Moved != 0 || j.Status != "running" {
		t.Fatalf("while blocked: %+v, want running with no progress", j)
	}
	if err := userTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	j := assertCompleted(t, pool, jobID)
	// Each lock timeout released the job and it was claimed again.
	if j.Moved != 999 || j.Skipped != 1 || j.Claims < 2 || j.Attempts != 0 {
		t.Fatalf("moved=%d skipped=%d claims=%d attempts=%d, want 999/1/>=2/0", j.Moved, j.Skipped, j.Claims, j.Attempts)
	}
	if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE id = $1 AND stage_id = $2`, first, s3); n != 1 {
		t.Fatal("user's edit was overwritten")
	}
}
