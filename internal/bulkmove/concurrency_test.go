package bulkmove

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"opps/internal/platform"
	"opps/internal/platform/testdb"
)

// ---------------------------------------------------------------------------
// 3. Concurrency: users vs. the job, and workers vs. each other
// ---------------------------------------------------------------------------

// A deal edited after submit (version bumped) is skipped, even though it
// would still be eligible: the user's later explicit action wins.
// Fails if the `o.version = i.snapshot_version` guard is removed.
func TestUserEditAfterSubmitWins(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 1000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)

	ids := oppIDs(t, pool, s1)
	edited := []int64{}
	for i := 0; i < 1000; i += 100 { // 10 deals spread over 10 chunks
		if err := manualMove(ctx, pool, ws, ids[i], s3); err != nil {
			t.Fatal(err)
		}
		edited = append(edited, ids[i])
	}

	startWorker(t, newWorker(t, NewRepository(pool), testConfig()))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))

	j := assertCompleted(t, pool, jobID)
	if j.Moved != 990 || j.Skipped != 10 {
		t.Fatalf("moved=%d skipped=%d, want 990/10", j.Moved, j.Skipped)
	}
	if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE id = ANY($1) AND stage_id = $2`, edited, s3); n != 10 {
		t.Fatalf("%d of 10 edited deals still in the user's stage", n)
	}
}

// A user's transaction holds a deal's row lock while the chunk reaches it.
// The chunk waits; when the user commits, Postgres re-checks the WHERE
// against the new row version, sees version changed, and excludes the deal.
// Fails if the version guard is removed, or if the chunk runs under
// REPEATABLE READ (the wait would abort the chunk instead of re-checking).
func TestUserEditDuringChunkWins(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := map[bool]string{true: "user commits", false: "user rolls back"}[commit]
		t.Run(name, func(t *testing.T) {
			pool := testdb.New(t)
			ctx := context.Background()
			ws, s1, s2 := seedWorkspace(t, pool, "A")
			s3 := addStage(t, pool, ws, 3)
			insertOpps(t, pool, ws, s1, 1000)
			jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
			first := oppIDs(t, pool, s1)[0]

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
			cfg.LockTimeout = 5 * time.Second // wait for the user, don't time out
			cfg.LeaseTTL = 10 * time.Second
			repo := NewRepository(pool)
			job, err := repo.Claim(ctx, "token", cfg.LeaseTTL)
			if err != nil || job == nil {
				t.Fatalf("claim = %+v, %v", job, err)
			}

			// Drive chunk 1 directly so we see its own result, not a retry's.
			chunkErr := make(chan error, 1)
			go func() { _, err := repo.ApplyChunk(ctx, job, cfg); chunkErr <- err }()

			// Deterministic: wait until the chunk is actually blocked on our lock.
			waitFor(t, 10*time.Second, "chunk blocked on the user's row lock", func() bool {
				return count(t, pool, `
					SELECT count(*) FROM pg_stat_activity
					WHERE datname = current_database() AND wait_event_type = 'Lock'`) > 0
			})
			time.Sleep(100 * time.Millisecond)
			if commit {
				err = userTx.Commit(ctx)
			} else {
				err = userTx.Rollback(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}

			// The SAME chunk attempt must succeed after the wait. Under
			// REPEATABLE READ it would fail here with a serialization error.
			if err := <-chunkErr; err != nil {
				t.Fatalf("chunk 1 after waiting on the user: %v", err)
			}
			j := loadJob(t, pool, jobID)
			wantSkipped := map[bool]int{true: 1, false: 0}[commit]
			if j.Cursor != 100 || j.Skipped != wantSkipped || j.Moved != 100-wantSkipped {
				t.Fatalf("after chunk 1: %+v, want cursor=100 skipped=%d", j, wantSkipped)
			}

			for done := false; !done; {
				if done, err = repo.ApplyChunk(ctx, job, cfg); err != nil {
					t.Fatal(err)
				}
			}
			j = assertCompleted(t, pool, jobID)
			wantMoved, wantStage := 999, s3
			if !commit {
				wantMoved, wantStage = 1000, s2 // user changed nothing: deal is moved
			}
			if j.Moved != wantMoved || j.Skipped != 1000-wantMoved {
				t.Fatalf("moved=%d skipped=%d, want %d/%d", j.Moved, j.Skipped, wantMoved, 1000-wantMoved)
			}
			if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE id = $1 AND stage_id = $2`, first, wantStage); n != 1 {
				t.Fatalf("first deal not in stage %d", wantStage)
			}
		})
	}
}

// Many users move random deals while the job runs. Whatever the
// interleaving: the job never moves a deal after a user moved it (version
// guard), never moves a deal twice, every item is accounted for, and every
// deal's current stage matches its last recorded transition.
func TestConcurrentManualMovesDuringJob(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 3000)
	ids := oppIDs(t, pool, s1)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)

	cfg := testConfig()
	cfg.ChunkPause = 2 * time.Millisecond
	cfg.LockTimeout = 2 * time.Second
	cfg.LeaseTTL = 10 * time.Second
	startWorker(t, newWorker(t, NewRepository(pool), cfg))

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for range 100 {
				id := ids[rng.IntN(len(ids))]
				to := []int64{s1, s3}[rng.IntN(2)]
				if err := manualMove(ctx, pool, ws, id, to); err != nil {
					errs <- fmt.Errorf("manual move %d: %w", id, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	j := assertCompleted(t, pool, jobID)

	// Version guard under concurrency: all manual moves happened after
	// submit, so no deal may have a bulk transition after a manual one.
	if n := count(t, pool, `
		SELECT count(*) FROM stage_transitions b
		JOIN stage_transitions m ON m.opportunity_id = b.opportunity_id
		WHERE b.bulk_job_id = $1 AND m.source = 'manual' AND m.id < b.id`, jobID); n != 0 {
		t.Fatalf("%d deals moved by the job after a user had moved them", n)
	}
	// History agrees with state for every deal that has any transition.
	if n := count(t, pool, `
		SELECT count(*) FROM opportunities o
		JOIN LATERAL (SELECT to_stage_id FROM stage_transitions
		              WHERE opportunity_id = o.id ORDER BY id DESC LIMIT 1) last ON true
		WHERE o.workspace_id = $1 AND o.stage_id <> last.to_stage_id`, ws); n != 0 {
		t.Fatalf("%d deals whose stage disagrees with their latest transition", n)
	}
	// Make sure the test exercised real interleaving, not two serial phases.
	editedNotMoved := count(t, pool, `
		SELECT count(DISTINCT m.opportunity_id) FROM stage_transitions m
		JOIN bulk_job_items i ON i.opportunity_id = m.opportunity_id AND i.job_id = $1
		WHERE m.source = 'manual'
		  AND NOT EXISTS (SELECT 1 FROM stage_transitions b
		                  WHERE b.opportunity_id = m.opportunity_id AND b.bulk_job_id = $1)`, jobID)
	t.Logf("moved=%d skipped=%d; deals edited by users and therefore skipped=%d", j.Moved, j.Skipped, editedNotMoved)
	if j.Skipped == 0 || j.Skipped == j.Total {
		t.Fatalf("skipped=%d of %d: users and the job did not interleave", j.Skipped, j.Total)
	}
	// The only reason to skip here is a user edit, and every user-edited
	// deal the job didn't move must have been skipped: the two sets match.
	if j.Skipped != editedNotMoved {
		t.Fatalf("skipped=%d but %d user-edited deals were left alone by the job", j.Skipped, editedNotMoved)
	}
}

// A worker whose lease was taken over cannot write anything, and cannot
// mark the job failed. Fails if the lease_token fence is removed from
// ApplyChunk or MarkFailed.
func TestReplacedWorkerCannotWrite(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 500)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	repo := NewRepository(pool)
	cfg := testConfig()

	a, err := repo.Claim(ctx, "token-a", cfg.LeaseTTL)
	if err != nil || a == nil || a.ID != jobID {
		t.Fatalf("A claim = %+v, %v", a, err)
	}
	// A stalls past its lease; B takes the job over.
	if _, err := pool.Exec(ctx, `UPDATE bulk_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	b, err := repo.Claim(ctx, "token-b", cfg.LeaseTTL)
	if err != nil || b == nil || b.ID != jobID {
		t.Fatalf("B claim = %+v, %v", b, err)
	}

	if _, err := repo.ApplyChunk(ctx, a, cfg); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A ApplyChunk err = %v, want ErrLeaseLost", err)
	}
	if ok, err := repo.MarkFailed(ctx, a, "stale"); err != nil || ok {
		t.Fatalf("A MarkFailed = %v, %v; want false", ok, err)
	}
	j := loadJob(t, pool, jobID)
	if j.Cursor != 0 || j.Moved != 0 || j.Status != "running" || j.Claims != 2 || bulkTransitions(t, pool, jobID) != 0 {
		t.Fatalf("after stale writes: %+v", j)
	}

	// B, the real owner, proceeds normally.
	for {
		done, err := repo.ApplyChunk(ctx, b, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if j := assertCompleted(t, pool, jobID); j.Moved != 500 {
		t.Fatalf("moved = %d, want 500", j.Moved)
	}
}

// Several goroutines applying chunks with the SAME token at once (e.g. a
// bug or a retry racing the original) are serialized by the fence row
// lock: every chunk is applied exactly once.
func TestConcurrentApplyChunkSameLeaseAppliesOnce(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 2000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	repo := NewRepository(pool)
	cfg := testConfig()
	cfg.LeaseTTL = 10 * time.Second

	job, err := repo.Claim(ctx, "token", cfg.LeaseTTL)
	if err != nil || job == nil {
		t.Fatalf("claim = %+v, %v", job, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				done, err := repo.ApplyChunk(ctx, job, cfg)
				if done || errors.Is(err, ErrLeaseLost) { // lease cleared on completion
					return
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if j := assertCompleted(t, pool, jobID); j.Moved != 2000 || j.Skipped != 0 {
		t.Fatalf("moved=%d skipped=%d, want 2000/0", j.Moved, j.Skipped)
	}
}

// 8 workers race to claim 1 pending job: exactly one wins.
// Fails without FOR UPDATE SKIP LOCKED in Claim.
func TestConcurrentClaimsOneWinner(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 10)
	for i := range 20 { // the race window is small; repeat to make it bite
		if _, err := pool.Exec(context.Background(), `DELETE FROM bulk_jobs`); err != nil {
			t.Fatal(err)
		}
		submitJob(t, pool, ws, fmt.Sprintf("k%d", i), Filter{StageID: &s1}, s2)

		if wins := raceClaims(t, NewRepository(pool), 8); wins != 1 {
			t.Fatalf("round %d: %d claims succeeded, want 1", i, wins)
		}
		if c := count(t, pool, `SELECT claims FROM bulk_jobs`); c != 1 {
			t.Fatalf("round %d: claims = %d, want 1", i, c)
		}
	}
}

// raceClaims has n goroutines call Claim at once; returns how many got a job.
func raceClaims(t *testing.T, repo *Repository, n int) int {
	t.Helper()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		wins  int
		start = make(chan struct{})
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			job, err := repo.Claim(context.Background(), fmt.Sprintf("token-%d", i), 30*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			if job != nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	return wins
}

// Two worker instances (4 goroutines each, each with its own pool sized
// Workers+1, like cmd/worker) drain two jobs in each of four workspaces.
// Every job completes with no takeovers, no deal is moved twice by one job,
// transitions = moved for every job, every deal ends in the target, and no
// workspace ever runs two jobs at the same time.
func TestManyWorkersManyJobs(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	cfg := testConfig()
	cfg.Workers = 4
	cfg.LeaseTTL = 10 * time.Second // pool contention must not look like a crash

	type wsJobs struct {
		ws, target    int64
		first, second int64
	}
	var all []wsJobs
	for i := range 4 {
		ws, s1, s2 := seedWorkspace(t, pool, fmt.Sprintf("W%d", i))
		s3 := addStage(t, pool, ws, 3)
		insertOpps(t, pool, ws, s1, 1000)
		insertOpps(t, pool, ws, s3, 500)
		all = append(all, wsJobs{
			ws: ws, target: s2,
			first:  submitJob(t, pool, ws, "k1", Filter{StageID: &s1}, s2),
			second: submitJob(t, pool, ws, "k2", Filter{StageID: &s3}, s2),
		})
	}
	for i := range 2 {
		wp, err := platform.NewPool(ctx, pool.Config().ConnString(), int32(cfg.Workers+1), fmt.Sprintf("bulk-worker-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(wp.Close)
		// Registered after the pool's Close, so the worker stops first.
		startWorker(t, newWorker(t, NewRepository(wp), cfg))
	}
	waitFor(t, 60*time.Second, "all jobs completed", func() bool {
		return count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE status = 'completed'`) == 8
	})

	for _, w := range all {
		a, b := assertCompleted(t, pool, w.first), assertCompleted(t, pool, w.second)
		if a.Moved != 1000 || b.Moved != 500 || a.Skipped != 0 || b.Skipped != 0 {
			t.Fatalf("jobs %d/%d: moved %d/%d skipped %d/%d, want 1000/500 0/0",
				w.first, w.second, a.Moved, b.Moved, a.Skipped, b.Skipped)
		}
		if a.Claims != 1 || b.Claims != 1 {
			t.Fatalf("jobs %d/%d: claims %d/%d, want 1/1 (no takeovers)", w.first, w.second, a.Claims, b.Claims)
		}
		if !b.StartedAt.After(*a.FinishedAt) && !a.StartedAt.After(*b.FinishedAt) {
			t.Fatalf("jobs %d and %d in one workspace overlapped: %v–%v and %v–%v",
				w.first, w.second, a.StartedAt, a.FinishedAt, b.StartedAt, b.FinishedAt)
		}
		if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE workspace_id = $1 AND stage_id <> $2`, w.ws, w.target); n != 0 {
			t.Fatalf("workspace %d: %d deals not in the target stage", w.ws, n)
		}
	}

	// Across all jobs at once: transitions = moved per job, and no
	// (job, deal) pair recorded twice.
	if n := count(t, pool, `
		SELECT count(*) FROM bulk_jobs j
		WHERE j.moved <> (SELECT count(*) FROM stage_transitions s WHERE s.bulk_job_id = j.id)`); n != 0 {
		t.Fatalf("%d jobs whose transition count differs from moved", n)
	}
	if n := count(t, pool, `
		SELECT count(*) FROM (SELECT 1 FROM stage_transitions WHERE bulk_job_id IS NOT NULL
		GROUP BY bulk_job_id, opportunity_id HAVING count(*) > 1) d`); n != 0 {
		t.Fatalf("%d deals moved more than once by one job", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM stage_transitions WHERE source = 'bulk'`); n != 4*1500 {
		t.Fatalf("%d bulk transitions in total, want %d", n, 4*1500)
	}
}

// The job moves a deal first and a user moves it afterwards: the user's
// move applies on top of the job's, the final stage is the user's, and the
// deal's history records both transitions in order. The job never comes
// back to the deal (its cursor is past it). Checked both while the job is
// still running and after it completed.
func TestUserMoveAfterJobMovedDealApplies(t *testing.T) {
	for _, midJob := range []bool{true, false} {
		name := map[bool]string{true: "job still running", false: "job completed"}[midJob]
		t.Run(name, func(t *testing.T) {
			pool := testdb.New(t)
			ctx := context.Background()
			ws, s1, s2 := seedWorkspace(t, pool, "A")
			s3 := addStage(t, pool, ws, 3)
			insertOpps(t, pool, ws, s1, 500)
			deal := oppIDs(t, pool, s1)[0] // in chunk 1
			jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
			repo := NewRepository(pool)
			cfg := testConfig()
			cfg.LeaseTTL = 10 * time.Second

			job, err := repo.Claim(ctx, "token", cfg.LeaseTTL)
			if err != nil || job == nil {
				t.Fatalf("claim = %+v, %v", job, err)
			}
			finish := func() {
				for done := false; !done; {
					if done, err = repo.ApplyChunk(ctx, job, cfg); err != nil {
						t.Fatal(err)
					}
				}
			}
			if midJob {
				if _, err := repo.ApplyChunk(ctx, job, cfg); err != nil {
					t.Fatal(err)
				}
			} else {
				finish()
			}
			if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE id = $1 AND stage_id = $2`, deal, s2); n != 1 {
				t.Fatal("the job did not move the deal first")
			}

			if err := manualMove(ctx, pool, ws, deal, s3); err != nil {
				t.Fatal(err)
			}
			if midJob {
				finish()
			}

			j := assertCompleted(t, pool, jobID)
			if j.Moved != 500 || j.Skipped != 0 {
				t.Fatalf("moved=%d skipped=%d, want 500/0", j.Moved, j.Skipped)
			}
			if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE id = $1 AND stage_id = $2`, deal, s3); n != 1 {
				t.Fatal("deal is not in the user's stage")
			}

			type tr struct {
				from, to int64
				source   string
				job      *int64
			}
			rows, err := pool.Query(ctx, `
				SELECT from_stage_id, to_stage_id, source, bulk_job_id
				FROM stage_transitions WHERE opportunity_id = $1 ORDER BY id`, deal)
			if err != nil {
				t.Fatal(err)
			}
			var got []tr
			for rows.Next() {
				var r tr
				if err := rows.Scan(&r.from, &r.to, &r.source, &r.job); err != nil {
					t.Fatal(err)
				}
				got = append(got, r)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 ||
				got[0].source != "bulk" || got[0].from != s1 || got[0].to != s2 || got[0].job == nil || *got[0].job != jobID ||
				got[1].source != "manual" || got[1].from != s2 || got[1].to != s3 || got[1].job != nil {
				t.Fatalf("transitions = %+v, want [bulk %d→%d (job %d), manual %d→%d]", got, s1, s2, jobID, s2, s3)
			}
		})
	}
}

// Two jobs in one workspace whose snapshots overlap, both submitted before
// either runs. They run one after the other; the second skips every deal
// the first already moved (its snapshot version is stale) and moves only
// the rest. Counts add up and no deal is moved by both.
func TestOverlappingJobsSecondSkipsMovedDeals(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	s4 := addStage(t, pool, ws, 4)
	insertOpps(t, pool, ws, s1, 1000) // matched by both jobs
	insertOpps(t, pool, ws, s4, 500)  // matched by the second job only

	first := submitJob(t, pool, ws, "k1", Filter{StageID: &s1}, s2)
	// Whole workspace, to a different stage than the first job, so the only
	// reason to skip is the first job's version bump.
	second := submitJob(t, pool, ws, "k2", Filter{}, s3)

	cfg := testConfig()
	cfg.Workers = 2
	cfg.LeaseTTL = 10 * time.Second
	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "both completed", func() bool {
		return count(t, pool, `SELECT count(*) FROM bulk_jobs WHERE status = 'completed'`) == 2
	})

	a, b := assertCompleted(t, pool, first), assertCompleted(t, pool, second)
	if b.StartedAt.Before(*a.FinishedAt) {
		t.Fatalf("second job started %v before the first finished %v", b.StartedAt, a.FinishedAt)
	}
	if a.Total != 1000 || a.Moved != 1000 || a.Skipped != 0 {
		t.Fatalf("first: %+v, want total=1000 moved=1000 skipped=0", a)
	}
	if b.Total != 1500 || b.Moved != 500 || b.Skipped != 1000 {
		t.Fatalf("second: %+v, want total=1500 moved=500 skipped=1000", b)
	}
	if a.Moved+b.Moved != 1500 || b.Skipped != a.Moved {
		t.Fatalf("counts don't add up: moved %d+%d, second skipped %d", a.Moved, b.Moved, b.Skipped)
	}
	if n := count(t, pool, `
		SELECT count(*) FROM (SELECT opportunity_id FROM stage_transitions
		WHERE bulk_job_id IN ($1, $2) GROUP BY 1 HAVING count(*) > 1) d`, first, second); n != 0 {
		t.Fatalf("%d deals moved by both jobs", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE workspace_id = $1 AND stage_id = $2`, ws, s2); n != 1000 {
		t.Fatalf("%d deals in the first job's target, want 1000", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE workspace_id = $1 AND stage_id = $2`, ws, s3); n != 500 {
		t.Fatalf("%d deals in the second job's target, want 500", n)
	}
}
