package bulkmove

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// 6. Transactions and consistency
//
// 36. A submit that fails partway (too many matches) leaves no job and no
// items: TestSubmitTooManyCreatesNothing (submit_test.go) and, over HTTP
// with concurrent requests, TestIdemConcurrentTooManyMatches.
//
// 37. Every test that opens its database with newDB runs checkInvariants
// when it ends (worker_test.go).
// ---------------------------------------------------------------------------

// committedState fingerprints everything a chunk could write, as other
// sessions see it: the job row (progress, counters, lease), every deal
// (stage, version, updated_at) and the transitions table (row count and
// highest id). Equal fingerprints = nothing committed in between.
func committedState(ctx context.Context, pool *pgxpool.Pool, jobID int64) (string, error) {
	var s string
	err := pool.QueryRow(ctx, `
		SELECT md5(
		  (SELECT row(cursor_seq, moved, skipped, status, lease_token, lease_expires_at,
		              last_progress_at, last_error)::text
		   FROM bulk_jobs WHERE id = $1)
		  || '|' || (SELECT coalesce(string_agg(id || ':' || stage_id || ':' || version || ':' || updated_at,
		                                       ',' ORDER BY id), '') FROM opportunities)
		  || '|' || (SELECT count(*) || ':' || coalesce(max(id), 0) FROM stage_transitions))`,
		jobID).Scan(&s)
	return s, err
}

// 34. An error just before COMMIT (all of chunk 3's writes done) leaves no
// trace: the committed state when the worker retries chunk 3 is exactly
// the state before the failed attempt. The worker's own retry then
// succeeds and the job completes with nothing doubled.
//
// Both fingerprints are taken from inside the chunk transaction (the hook)
// through another connection, so they see only committed data: "before" is
// taken during the failing attempt, "after" during the retry.
func TestErrorBeforeCommitLeavesNoTrace(t *testing.T) {
	pool := newDB(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000) // 10 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)

	var (
		mu            sync.Mutex
		calls         int
		before, after string
		hookErr       error
		injected      = errors.New("injected failure before COMMIT")
	)
	repo := NewRepository(pool)
	repo.beforeCommit = func() error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		switch calls {
		case 3: // chunk 3, first attempt: fail after all writes
			before, hookErr = committedState(context.Background(), pool, jobID)
			return injected
		case 4: // chunk 3, retry: what did the failed attempt leave?
			after, hookErr = committedState(context.Background(), pool, jobID)
		}
		return nil
	}
	startWorker(t, newWorker(t, repo, testConfig()))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))

	mu.Lock()
	defer mu.Unlock()
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if before == "" || after != before {
		t.Fatalf("failed attempt left a trace: committed state before %q, at retry %q", before, after)
	}
	if calls != 11 {
		t.Fatalf("chunk attempts = %d, want 11 (10 chunks + 1 retry)", calls)
	}
	j := assertCompleted(t, pool, jobID)
	if j.Moved != 1000 || j.Skipped != 0 || j.Claims != 1 || j.LastError != nil {
		t.Fatalf("job = %+v, want moved=1000 skipped=0 claims=1 no last_error", j)
	}
}

// 35. The worker's database connection is killed (pg_terminate_backend)
// while chunk 3 is open with all its writes done. The server aborts the
// transaction: nothing of chunk 3 is committed. The worker sees the commit
// fail, retries on a fresh pooled connection, and the job completes with
// every deal moved exactly once, by the same worker (no takeover).
func TestConnectionKilledMidChunk(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000) // 10 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.LeaseTTL = 10 * time.Second
	cfg.RetryBackoff = 100 * time.Millisecond

	inChunk, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	calls := 0
	repo := NewRepository(pool)
	repo.beforeCommit = func() error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 3 {
			close(inChunk)
			<-release
		}
		return nil
	}
	startWorker(t, newWorker(t, repo, cfg))
	select {
	case <-inChunk:
	case <-time.After(30 * time.Second):
		t.Fatal("chunk 3 never started")
	}

	before, err := committedState(ctx, pool, jobID)
	if err != nil {
		t.Fatal(err)
	}
	// The only open transaction in this database is the worker's chunk.
	var pid int
	if err := pool.QueryRow(ctx, `
		SELECT pid FROM pg_stat_activity
		WHERE datname = current_database() AND state = 'idle in transaction'
		  AND pid <> pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatalf("find the worker's connection: %v", err)
	}
	var killed bool
	if err := pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&killed); err != nil || !killed {
		t.Fatalf("pg_terminate_backend(%d) = %v, %v", pid, killed, err)
	}
	waitFor(t, 10*time.Second, "backend gone", func() bool {
		return count(t, pool, `SELECT count(*) FROM pg_stat_activity WHERE pid = $1`, pid) == 0
	})

	after, err := committedState(ctx, pool, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("killed chunk left committed changes")
	}
	if j := loadJob(t, pool, jobID); j.Cursor != 200 || j.Moved != 200 {
		t.Fatalf("after kill: %+v, want cursor=moved=200", j)
	}
	close(release) // the worker's COMMIT now hits a dead connection

	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	j := assertCompleted(t, pool, jobID)
	if j.Moved != 1000 || j.Skipped != 0 || j.Claims != 1 || j.LastError != nil {
		t.Fatalf("job = %+v, want moved=1000 skipped=0 claims=1 no last_error", j)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 11 {
		t.Fatalf("chunk attempts = %d, want 11 (10 chunks + 1 retry)", calls)
	}
}
