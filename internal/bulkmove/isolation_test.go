package bulkmove

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/platform/testdb"
)

// ---------------------------------------------------------------------------
// 4. Isolation across workspaces
// ---------------------------------------------------------------------------

// Two jobs in one workspace with two free workers: they still run one after
// the other. [isolation: one running job per workspace]
func TestOneRunningJobPerWorkspace(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 1000)
	insertOpps(t, pool, ws, s3, 1000)
	job1 := submitJob(t, pool, ws, "k1", Filter{StageID: &s1}, s2)
	job2 := submitJob(t, pool, ws, "k2", Filter{StageID: &s3}, s2)

	cfg := testConfig()
	cfg.Workers = 2
	cfg.ChunkPause = 5 * time.Millisecond
	startWorker(t, newWorker(t, NewRepository(pool), cfg))

	// Sample while running: never two running jobs in the workspace.
	deadline := time.Now().Add(30 * time.Second)
	for count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE status = 'completed'`) < 2 {
		if n := count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE workspace_id = $1 AND status = 'running'`, ws); n > 1 {
			t.Fatalf("%d jobs running in one workspace", n)
		}
		if time.Now().After(deadline) {
			t.Fatal("jobs did not complete")
		}
		time.Sleep(5 * time.Millisecond)
	}
	a, b := assertCompleted(t, pool, job1), assertCompleted(t, pool, job2)
	if b.StartedAt.Before(*a.FinishedAt) {
		t.Fatalf("job2 started %v before job1 finished %v", b.StartedAt, a.FinishedAt)
	}
}

// Workers racing to start two pending jobs of the same workspace: exactly
// one may win. NOT EXISTS alone is racy (both claimers can see "no running
// job"); the partial unique index bulk_jobs_one_running is the backstop and
// the loser's 23505 is reported as "nothing to claim".
// Fails if bulk_jobs_one_running is dropped (run with -count to see it).
func TestConcurrentClaimsSameWorkspaceOneWinner(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	for i := range 20 { // the race window is small; repeat to make it bite
		if _, err := pool.Exec(context.Background(), `DELETE FROM bulk_jobs`); err != nil {
			t.Fatal(err)
		}
		submitJob(t, pool, ws, fmt.Sprintf("a%d", i), Filter{StageID: &s1}, s2)
		submitJob(t, pool, ws, fmt.Sprintf("b%d", i), Filter{StageID: &s3}, s2)

		if wins := raceClaims(t, NewRepository(pool), 8); wins != 1 {
			t.Fatalf("round %d: %d claims succeeded for one workspace, want 1", i, wins)
		}
	}
}

// Jobs in different workspaces run at the same time: one workspace's job
// never waits for another's.
func TestDifferentWorkspacesRunInParallel(t *testing.T) {
	pool := testdb.New(t)
	var jobs []int64
	for _, name := range []string{"A", "B"} {
		ws, s1, s2 := seedWorkspace(t, pool, name)
		insertOpps(t, pool, ws, s1, 2000)
		jobs = append(jobs, submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2))
	}
	cfg := testConfig()
	cfg.Workers = 2
	cfg.ChunkPause = 20 * time.Millisecond // ~20 chunks × 20ms: long enough to overlap
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "both completed", func() bool {
		return count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE status = 'completed'`) == 2
	})

	a, b := assertCompleted(t, pool, jobs[0]), assertCompleted(t, pool, jobs[1])
	if !a.StartedAt.Before(*b.FinishedAt) || !b.StartedAt.Before(*a.FinishedAt) {
		t.Fatalf("jobs did not overlap: A %v–%v, B %v–%v", a.StartedAt, a.FinishedAt, b.StartedAt, b.FinishedAt)
	}
}

// A big workspace with a queue of jobs occupies at most one worker slot. A
// small workspace's job submitted later is claimed past the queued (but
// blocked) job and finishes long before the big workspace's queue drains.
func TestBusyWorkspaceDoesNotStarveOthers(t *testing.T) {
	pool := testdb.New(t)
	big, b1, b2 := seedWorkspace(t, pool, "Big")
	b3 := addStage(t, pool, big, 3)
	insertOpps(t, pool, big, b1, 5000)
	insertOpps(t, pool, big, b3, 1000)
	bigJob1 := submitJob(t, pool, big, "k1", Filter{StageID: &b1}, b2)
	bigJob2 := submitJob(t, pool, big, "k2", Filter{StageID: &b3}, b2) // older than the small job

	small, s1, s2 := seedWorkspace(t, pool, "Small")
	insertOpps(t, pool, small, s1, 200)
	smallJob := submitJob(t, pool, small, "k", Filter{StageID: &s1}, s2)

	cfg := testConfig()
	cfg.Workers = 2
	cfg.ChunkPause = 10 * time.Millisecond
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 60*time.Second, "all completed", func() bool {
		return count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE status = 'completed'`) == 3
	})

	j1, j2, s := assertCompleted(t, pool, bigJob1), assertCompleted(t, pool, bigJob2), assertCompleted(t, pool, smallJob)
	if !s.FinishedAt.Before(*j1.FinishedAt) {
		t.Fatalf("small job finished %v, after big job 1 %v: it was starved", s.FinishedAt, j1.FinishedAt)
	}
	if j2.StartedAt.Before(*j1.FinishedAt) {
		t.Fatal("big workspace ran two jobs at once")
	}
}

// Users in the workspace being bulk-moved are never stuck behind the job
// for long: a chunk holds its row locks only for its own short transaction.
// [isolation: chunk pacing + short transactions]
func TestUserMovesStayFastDuringBulkJob(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 10000)
	ids := oppIDs(t, pool, s1)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)

	cfg := DefaultConfig() // production chunking: 500 rows, 50ms pause
	cfg.Workers = 1
	cfg.PollEvery = 20 * time.Millisecond
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 10*time.Second, "job running", statusIs(t, pool, jobID, "running"))

	rng := rand.New(rand.NewPCG(1, 2))
	var lat []time.Duration
	for loadJob(t, pool, jobID).Status == "running" && len(lat) < 200 {
		id := ids[rng.IntN(len(ids))]
		start := time.Now()
		if err := manualMove(ctx, pool, ws, id, s3); err != nil {
			t.Fatal(err)
		}
		lat = append(lat, time.Since(start))
		time.Sleep(5 * time.Millisecond)
	}
	if len(lat) < 20 {
		t.Fatalf("only %d user moves overlapped the job; make the job longer", len(lat))
	}
	slices.Sort(lat)
	p50, max := lat[len(lat)/2], lat[len(lat)-1]
	t.Logf("%d user moves during the job: p50=%v max=%v", len(lat), p50, max)
	if max > time.Second {
		t.Fatalf("a user move waited %v behind the bulk job", max)
	}
	waitFor(t, 60*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	assertCompleted(t, pool, jobID)
}

// wsFingerprint hashes every deal of the workspace (id, stage, version,
// updated_at): any write to any of its rows changes it.
func wsFingerprint(t *testing.T, pool *pgxpool.Pool, ws int64) string {
	t.Helper()
	var h string
	if err := pool.QueryRow(context.Background(), `
		SELECT coalesce(md5(string_agg(id || ':' || stage_id || ':' || version || ':' || updated_at, ',' ORDER BY id)), '')
		FROM opportunities WHERE workspace_id = $1`, ws).Scan(&h); err != nil {
		t.Fatal(err)
	}
	return h
}

// Tenant isolation: jobs in workspace A never touch workspace B's rows —
// not with an empty (whole-workspace) filter, not with a filter naming B's
// stage, and not even if A's snapshot somehow contained a B deal (the chunk
// re-checks workspace_id). B cannot read A's job: it gets the same 404 as
// for a job that does not exist.
func TestTenantIsolation(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	app := newApp(pool)
	wsA, a1, a2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, wsA, a1, 300)
	insertOpps(t, pool, wsA, a2, 200) // already in the target: skipped
	wsB, b1, _ := seedWorkspace(t, pool, "B")
	insertOpps(t, pool, wsB, b1, 300)
	before := wsFingerprint(t, pool, wsB)

	allOfA := submitJob(t, pool, wsA, "all", Filter{}, a2)
	foreignStage := submitJob(t, pool, wsA, "foreign", Filter{StageID: &b1}, a2)
	if n := count(t, pool, `
		SELECT count(*) FROM bulk_job_items i JOIN opportunities o ON o.id = i.opportunity_id
		WHERE i.job_id IN ($1, $2) AND o.workspace_id <> $3`, allOfA, foreignStage, wsA); n != 0 {
		t.Fatalf("%d of B's deals in A's snapshots", n)
	}
	if j := loadJob(t, pool, foreignStage); j.Total != 0 {
		t.Fatalf("filter on B's stage matched %d deals in A", j.Total)
	}

	// Defence in depth: plant one of B's deals in A's snapshot, with a
	// matching version. The chunk must still skip it.
	bDeal := oppIDs(t, pool, b1)[0]
	if _, err := pool.Exec(ctx, `
		INSERT INTO bulk_job_items (job_id, seq, opportunity_id, snapshot_version)
		SELECT $1, 501, id, version FROM opportunities WHERE id = $2`, allOfA, bDeal); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE bulk_jobs SET total = 501 WHERE id = $1`, allOfA); err != nil {
		t.Fatal(err)
	}

	startWorker(t, newWorker(t, NewRepository(pool), testConfig()))
	waitFor(t, 30*time.Second, "both completed", func() bool {
		return count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE status = 'completed'`) == 2
	})
	if j := assertCompleted(t, pool, allOfA); j.Moved != 300 || j.Skipped != 201 {
		t.Fatalf("A's job: moved=%d skipped=%d, want 300/201", j.Moved, j.Skipped)
	}
	assertCompleted(t, pool, foreignStage)

	if after := wsFingerprint(t, pool, wsB); after != before {
		t.Fatal("A's jobs wrote to B's deals")
	}
	if n := count(t, pool, `
		SELECT count(*) FROM stage_transitions s JOIN opportunities o ON o.id = s.opportunity_id
		WHERE s.workspace_id = $1 OR o.workspace_id = $1`, wsB); n != 0 {
		t.Fatalf("%d transitions touch B", n)
	}

	// Reads: A sees its job, B gets a 404 identical to a missing job's.
	path := fmt.Sprintf("/bulk-moves/%d", allOfA)
	resp, raw := get(t, app, fmt.Sprint(wsA), path)
	if resp.StatusCode != 200 {
		t.Fatalf("A GET own job: %d %s", resp.StatusCode, raw)
	}
	var js JobStatus
	if err := json.Unmarshal(raw, &js); err != nil {
		t.Fatal(err)
	}
	if js.ID != allOfA || js.Status != "completed" || js.Total != 501 || js.Moved != 300 || js.Skipped != 201 || js.FinishedAt == nil {
		t.Fatalf("A GET own job = %+v", js)
	}

	for _, tc := range []struct {
		name, ws, path string
		status         int
		code           string
	}{
		{"B reads A's job", fmt.Sprint(wsB), path, 404, "bulk_move_not_found"},
		{"missing job", fmt.Sprint(wsB), "/bulk-moves/999999", 404, "bulk_move_not_found"},
		{"invalid id", fmt.Sprint(wsB), "/bulk-moves/abc", 400, "invalid_path_param"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, raw := get(t, app, tc.ws, tc.path)
			var e errBody
			_ = json.Unmarshal(raw, &e)
			if resp.StatusCode != tc.status || e.Error.Code != tc.code {
				t.Fatalf("GET %s as %s: %d %s, want %d %s", tc.path, tc.ws, resp.StatusCode, raw, tc.status, tc.code)
			}
			if strings.Contains(string(raw), "completed") || strings.Contains(string(raw), `"moved"`) {
				t.Fatalf("404 body leaks job data: %s", raw)
			}
		})
	}
}
