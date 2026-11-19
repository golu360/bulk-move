package bulkmove

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"opps/internal/platform"
)

// ---------------------------------------------------------------------------
// 7. Retry and error handling
//
// 38. Transient failures, attempt count reset by progress:
//     TestChunkRetryAfterTransientFailure (worker_test.go).
// 39. Permanent failure → failed with last_error, never claimed again:
//     TestChunkRetryExhaustedMarksJobFailed (worker_test.go).
// ---------------------------------------------------------------------------

// 41. Worker A fails chunk 3. It does not retry in place: it releases the
// job with a backoff and stops touching it. Nobody may claim the job until
// the backoff is over; then worker B claims it, runs chunk 3 onwards and
// completes it. A never runs the job again.
func TestRetryPickedUpByAnotherWorker(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000) // 10 chunks
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.LeaseTTL = 10 * time.Second
	cfg.RetryBackoff = time.Second

	var mu sync.Mutex
	callsA := 0
	repoA := NewRepository(pool)
	repoA.beforeCommit = func() error {
		mu.Lock()
		defer mu.Unlock()
		if callsA++; callsA == 3 {
			return errors.New("injected failure on worker A")
		}
		return nil
	}
	stopA := startWorker(t, newWorker(t, repoA, cfg))
	waitFor(t, 30*time.Second, "job released by A", func() bool {
		j := loadJob(t, pool, jobID)
		return j.LeaseToken == nil && j.Attempts == 1
	})
	var notBefore time.Time
	if err := pool.QueryRow(ctx, `SELECT lease_expires_at FROM bulk_jobs WHERE id = $1`, jobID).Scan(&notBefore); err != nil {
		t.Fatal(err)
	}
	j := loadJob(t, pool, jobID)
	if j.Status != "running" || j.Cursor != 200 || j.Claims != 1 || j.LastError == nil ||
		!strings.Contains(*j.LastError, "injected failure on worker A") {
		t.Fatalf("after A's failure: %+v, want running at cursor 200, released, last_error set", j)
	}

	// A keeps polling, but no longer runs the job: no in-place retry.
	time.Sleep(300 * time.Millisecond)
	stopA()
	mu.Lock()
	if callsA != 3 {
		mu.Unlock()
		t.Fatalf("worker A ran %d chunk attempts, want 3: it retried in place", callsA)
	}
	mu.Unlock()

	// B starts inside the backoff and must wait it out: while the DB clock
	// is before notBefore, claims stays 1 (read in the same statement).
	repoB, chunksB := countingRepo(pool)
	startWorker(t, newWorker(t, repoB, cfg))
	for {
		var claims int
		var inBackoff bool
		if err := pool.QueryRow(ctx, `SELECT claims, now() < $2 FROM bulk_jobs WHERE id = $1`,
			jobID, notBefore).Scan(&claims, &inBackoff); err != nil {
			t.Fatal(err)
		}
		if !inBackoff {
			break
		}
		if claims != 1 {
			t.Fatal("the job was claimed again before its backoff was over")
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitFor(t, 30*time.Second, "job completed by B", statusIs(t, pool, jobID, "completed"))

	j = assertCompleted(t, pool, jobID)
	if j.Moved != 1000 || j.Claims != 2 || j.Attempts != 0 || j.LastError != nil {
		t.Fatalf("job = %+v, want moved=1000 claims=2, attempts and last_error cleared", j)
	}
	if got := chunksB.Load(); got != 8 {
		t.Fatalf("worker B ran %d chunks, want 8 (chunk 3 onwards)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if callsA != 3 {
		t.Fatalf("worker A ran %d chunk attempts in all, want 3", callsA)
	}
}

// 42. Poison job: chunk 3 kills every worker that runs it (no error
// returned, no lease released, so nothing is recorded by the dying worker).
// Each takeover of an abandoned lease counts as a failed attempt; once
// MaxChunkFailures is reached, the next worker to claim the job marks it
// failed instead of running it, and nobody claims it again.
func TestPoisonJobFailsAfterMaxAttempts(t *testing.T) {
	pool := newDB(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()
	cfg.MaxChunkFailures = 3

	runCrashingWorker(t, pool, cfg, 3) // chunks 1-2 commit, dies in chunk 3
	for i := range 2 {                 // each later worker dies in its first chunk: chunk 3 again
		runCrashingWorker(t, pool, cfg, 1)
		if j := loadJob(t, pool, jobID); j.Status != "running" || j.Cursor != 200 || j.Attempts != i+1 {
			t.Fatalf("after crash %d: %+v, want running at cursor 200 with attempts %d", i+2, j, i+1)
		}
	}

	repo, chunks := countingRepo(pool)
	stop := startWorker(t, newWorker(t, repo, cfg))
	waitFor(t, 30*time.Second, "job failed", statusIs(t, pool, jobID, "failed"))
	time.Sleep(5 * cfg.PollEvery) // give it a chance to (wrongly) claim it again
	stop()

	j := loadJob(t, pool, jobID)
	if j.Cursor != 200 || j.Moved != 200 || j.Claims != 4 || j.Attempts != 3 || j.LeaseToken != nil || j.FinishedAt == nil {
		t.Fatalf("poison job = %+v, want failed at cursor 200, claims 4, attempts 3, no lease, finished", j)
	}
	if j.LastError == nil || !strings.Contains(*j.LastError, "abandoned 3 times at item 200") {
		t.Fatalf("last_error = %v", deref(j.LastError))
	}
	if n := chunks.Load(); n != 0 {
		t.Fatalf("the worker that failed the job ran %d chunks of it, want 0", n)
	}
	assertTransitionsMatch(t, pool, jobID, j)
}

// 43. The database is briefly unavailable to an idle worker: all its
// connections are terminated and new connections are refused for a
// moment. The worker's claims fail meanwhile; once the database accepts
// connections again it reconnects on its own and runs a new job to the end.
func TestWorkerReconnectsAfterDatabaseOutage(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000)

	const app = "bulk-worker-outage"
	wp, err := platform.NewPool(ctx, pool.Config().ConnString(), 3, app)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wp.Close) // registered before the worker's stop, so it runs after it
	cfg := testConfig()
	cfg.Workers = 2
	startWorker(t, newWorker(t, NewRepository(wp), cfg))

	// The outage is driven from the server's base database: a session
	// cannot refuse connections to the database it is connected to.
	var db string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&db); err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(ctx, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) }) // after the re-allow cleanup below
	workerBackends := func() int {
		var n int
		if err := admin.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND application_name = $2`,
			db, app).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	waitFor(t, 10*time.Second, "worker connected", func() bool { return workerBackends() > 0 })

	allow := func(on bool) {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`ALTER DATABASE %s WITH ALLOW_CONNECTIONS %t`, db, on)); err != nil {
			t.Fatal(err)
		}
	}
	allow(false)
	t.Cleanup(func() { allow(true) }) // also if the test fails mid-outage
	if _, err := admin.Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = $1 AND application_name = $2`, db, app); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "worker connections gone", func() bool { return workerBackends() == 0 })
	time.Sleep(10 * cfg.PollEvery) // the worker keeps trying, and is refused
	if n := workerBackends(); n != 0 {
		t.Fatalf("%d worker connections while the database refuses connections", n)
	}
	allow(true)

	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	waitFor(t, 30*time.Second, "job completed after the outage", statusIs(t, pool, jobID, "completed"))
	j := assertCompleted(t, pool, jobID)
	if j.Moved != 1000 || j.Claims != 1 || j.Attempts != 0 {
		t.Fatalf("job = %+v, want moved=1000 claims=1 attempts=0", j)
	}
}
