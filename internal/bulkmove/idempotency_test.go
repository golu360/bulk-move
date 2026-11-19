package bulkmove

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgconn"

	"opps/internal/platform/testdb"
)

// Idempotency scenarios, end to end over HTTP. Numbering follows the
// acceptance list; cases already covered elsewhere point to their test.

// serve runs app on a real TCP listener (needed for concurrent clients and
// for dropping a connection mid-request) and returns its base URL.
func serve(t *testing.T, app *fiber.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	return "http://" + ln.Addr().String()
}

type submitResult struct {
	status   int
	replayed bool
	location string
	job      Job
	code     string
}

var httpClient = &http.Client{Timeout: 120 * time.Second}

func post(t *testing.T, base string, ws int64, key, body string) submitResult {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/bulk-moves", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Workspace-Id", fmt.Sprint(ws))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Error(err)
		return submitResult{}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	// The replay header is either absent or exactly "true", and only ever
	// on a 202: a new job and every error response must not carry it.
	vals := resp.Header.Values(ReplayedHeader)
	r := submitResult{
		status:   resp.StatusCode,
		replayed: len(vals) == 1 && vals[0] == "true",
		location: resp.Header.Get("Location"),
	}
	if len(vals) > 0 && (!r.replayed || resp.StatusCode != http.StatusAccepted) {
		t.Errorf("status %d with %s = %q; want the header only as \"true\" on a 202 replay",
			resp.StatusCode, ReplayedHeader, vals)
	}
	if resp.StatusCode == http.StatusAccepted {
		_ = json.Unmarshal(raw, &r.job)
	} else {
		var eb errBody
		_ = json.Unmarshal(raw, &eb)
		r.code = eb.Error.Code
	}
	return r
}

func moveBody(filterStage, target int64) string {
	return fmt.Sprintf(`{"filter":{"stage_id":%d},"target_stage_id":%d}`, filterStage, target)
}

// ★ 1. Same request twice in a row → covered by TestSubmitHandlerReplay
// (202 both, same id and Location, second replayed, exactly one job row).

// ★ 2. 50 identical requests fired concurrently → all 202, one id, exactly
// one created and 49 replays; no 409, no 500.
func TestIdem50ConcurrentIdenticalRequests(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 2000)
	base := serve(t, newApp(pool))
	body := moveBody(s1, s2)

	const n = 50
	results := make([]submitResult, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = post(t, base, ws, "burst", body)
		}()
	}
	close(start)
	wg.Wait()

	created, replays := 0, 0
	for i, r := range results {
		if r.status != http.StatusAccepted {
			t.Fatalf("request %d: status %d code %q, want 202", i, r.status, r.code)
		}
		if r.job.ID != results[0].job.ID || r.location != results[0].location {
			t.Fatalf("request %d: job %d %s, want %d %s", i, r.job.ID, r.location, results[0].job.ID, results[0].location)
		}
		if r.replayed {
			replays++
		} else {
			created++
		}
	}
	if created != 1 || replays != n-1 {
		t.Fatalf("created=%d replays=%d, want 1/%d", created, replays, n-1)
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_jobs`); c != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", c)
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_job_items`); c != 2000 {
		t.Fatalf("bulk_job_items rows = %d, want 2000", c)
	}
}

// ★ 3. Lost response: the client sends the submit and drops the connection
// before reading the reply. The server still commits; the client's retry
// gets the same job, and nothing is duplicated.
func TestIdemLostResponseThenRetry(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 2000)
	base := serve(t, newApp(pool))
	body := moveBody(s1, s2)

	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "POST /bulk-moves HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\n"+
		"X-Workspace-Id: %d\r\nIdempotency-Key: lost\r\nContent-Length: %d\r\n\r\n%s", ws, len(body), body)
	_ = conn.Close() // never read the response

	// The server finished the request even though nobody was listening.
	waitFor(t, 10*time.Second, "job created by the lost request", func() bool {
		return count(t, pool, `SELECT count(*) FROM bulk_jobs`) == 1
	})
	jobID := int64(count(t, pool, `SELECT id FROM bulk_jobs`))

	r := post(t, base, ws, "lost", body)
	if r.status != http.StatusAccepted || !r.replayed || r.job.ID != jobID || r.job.Total != 2000 {
		t.Fatalf("retry = %+v, want 202 replay of job %d with total 2000", r, jobID)
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_jobs`); c != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", c)
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_job_items`); c != 2000 {
		t.Fatalf("bulk_job_items rows = %d, want 2000", c)
	}
}

// ★ 4–6. Replays during and after processing return the same job with its
// CURRENT status, never create a second job, and never restart it.
func TestIdemReplayThroughJobLifecycle(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 1000)
	insertOpps(t, pool, ws, s3, 300)
	base := serve(t, newApp(pool))
	repo := NewRepository(pool)
	cfg := testConfig()

	body := moveBody(s1, s2)
	first := post(t, base, ws, "life", body)
	if first.status != http.StatusAccepted || first.replayed || first.job.Status != "pending" {
		t.Fatalf("first submit = %+v", first)
	}
	jobID := first.job.ID

	replay := func(want string) {
		t.Helper()
		r := post(t, base, ws, "life", body)
		if r.status != http.StatusAccepted || !r.replayed || r.job.ID != jobID || r.job.Status != want ||
			r.location != fmt.Sprintf("/bulk-moves/%d", jobID) {
			t.Fatalf("replay = %+v, want 202 replay of job %d with status %s", r, jobID, want)
		}
		if c := count(t, pool, `SELECT count(*) FROM bulk_jobs`); c > 2 { // job + the failed-case job below
			t.Fatalf("replay created a job: %d rows", c)
		}
	}

	// ★ 4. While processing: claim and apply 3 of 10 chunks.
	job, err := repo.Claim(ctx, "token", cfg.LeaseTTL)
	if err != nil || job == nil {
		t.Fatalf("claim = %+v, %v", job, err)
	}
	for range 3 {
		if _, err := repo.ApplyChunk(ctx, job, cfg); err != nil {
			t.Fatal(err)
		}
	}
	replay("running")
	if j := loadJob(t, pool, jobID); j.Cursor != 300 || j.Claims != 1 {
		t.Fatalf("replay disturbed the running job: %+v", j)
	}

	// ★ 5. After completion: no new transitions, and it does not run again.
	for done := false; !done; {
		if done, err = repo.ApplyChunk(ctx, job, cfg); err != nil {
			t.Fatal(err)
		}
	}
	done := assertCompleted(t, pool, jobID)
	replay("completed")
	stop := startWorker(t, newWorker(t, NewRepository(pool), cfg))
	time.Sleep(5 * cfg.PollEvery) // a worker that wrongly re-ran it would claim it now
	stop()
	after := loadJob(t, pool, jobID)
	if after.Claims != done.Claims || !after.FinishedAt.Equal(*done.FinishedAt) || after.Moved != 1000 ||
		bulkTransitions(t, pool, jobID) != 1000 {
		t.Fatalf("completed job changed after replay: before %+v after %+v", done, after)
	}

	// ★ 6. After failure: status failed, not restarted. (Retrying a failed
	// move means submitting again with a NEW key.)
	failBody := moveBody(s3, s2)
	f := post(t, base, ws, "fail", failBody)
	fjob, err := repo.Claim(ctx, "token-f", cfg.LeaseTTL)
	if err != nil || fjob == nil || fjob.ID != f.job.ID {
		t.Fatalf("claim failed-case job = %+v, %v", fjob, err)
	}
	if ok, err := repo.MarkFailed(ctx, fjob, "simulated"); err != nil || !ok {
		t.Fatalf("mark failed = %v, %v", ok, err)
	}
	r := post(t, base, ws, "fail", failBody)
	if r.status != http.StatusAccepted || !r.replayed || r.job.ID != f.job.ID || r.job.Status != "failed" {
		t.Fatalf("replay of failed job = %+v", r)
	}
	stop = startWorker(t, newWorker(t, NewRepository(pool), cfg))
	time.Sleep(5 * cfg.PollEvery)
	stop()
	if j := loadJob(t, pool, f.job.ID); j.Status != "failed" || j.Claims != 1 || j.Moved != 0 {
		t.Fatalf("failed job was restarted: %+v", j)
	}
	// A new key is how you retry it.
	retry := post(t, base, ws, "fail-retry", failBody)
	if retry.status != http.StatusAccepted || retry.replayed || retry.job.ID == f.job.ID || retry.job.Total != 300 {
		t.Fatalf("retry with new key = %+v, want a new job of 300", retry)
	}
}

// 7. Same key, different body → 422 idempotency_key_reused, and the
// original job is untouched (same hash, filter, target, total, items).
func TestIdemKeyReusedLeavesOriginalUntouched(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	s3 := addStage(t, pool, ws, 3)
	insertOpps(t, pool, ws, s1, 500)
	base := serve(t, newApp(pool))

	first := post(t, base, ws, "k", moveBody(s1, s2))
	snapshot := func() string {
		var s string
		if err := pool.QueryRow(context.Background(), `
			SELECT encode(request_hash, 'hex') || filter::text || target_stage_id || '/' || total || '/' || status
			       || '/' || (SELECT count(*) FROM bulk_job_items WHERE job_id = j.id)
			FROM bulk_jobs j WHERE id = $1`, first.job.ID).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := snapshot()

	for _, body := range []string{moveBody(s1, s3), moveBody(s3, s2), `{"filter":{},"target_stage_id":` + fmt.Sprint(s2) + `}`} {
		r := post(t, base, ws, "k", body)
		if r.status != http.StatusUnprocessableEntity || r.code != "idempotency_key_reused" {
			t.Fatalf("body %s: %+v, want 422 idempotency_key_reused", body, r)
		}
	}
	if after := snapshot(); after != before {
		t.Fatalf("original job changed:\nbefore %s\nafter  %s", before, after)
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_jobs`); c != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", c)
	}
}

// 8. Different JSON field order / whitespace → replay: covered by
// TestSubmitHandlerReplay (reordered, reformatted body) and
// TestSubmitReplayIgnoresTimeZoneOffset (same instant, different offset).
//
// 9. Same key in another workspace → separate job: covered by
// TestSubmitKeyIsScopedToWorkspace.

// 10. 10 concurrent identical requests whose filter matches > 50k → all 422
// too_many_matches, zero job rows, no 409 or 500. (Each attempt inserts and
// rolls back; the next one blocked on the unique index then gets its turn.)
func TestIdemConcurrentTooManyMatches(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, MaxItems+1)
	base := serve(t, newApp(pool))
	body := moveBody(s1, s2)

	const n = 10
	results := make([]submitResult, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() { defer wg.Done(); results[i] = post(t, base, ws, "huge", body) }()
	}
	wg.Wait()
	for i, r := range results {
		if r.status != http.StatusUnprocessableEntity || r.code != "too_many_matches" {
			t.Fatalf("request %d: %d %q, want 422 too_many_matches", i, r.status, r.code)
		}
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_jobs`); c != 0 {
		t.Fatalf("bulk_jobs rows = %d, want 0", c)
	}
	if c := count(t, pool, `SELECT count(*) FROM bulk_job_items`); c != 0 {
		t.Fatalf("bulk_job_items rows = %d, want 0", c)
	}
}

// 11. Missing Idempotency-Key → 400: covered by TestSubmitHandlerErrors
// (missing, too long, non-ASCII).

// ★ 12. Idempotency through the lifecycle: rewind a completed job's cursor
// to 0 and run it again. Every deal's version moved past its snapshot
// version when the job moved it, so the rerun moves nothing and writes no
// transitions. Users moved 100 deals back out of the target afterwards, so
// the stage guard alone cannot save them: only the version guard does.
// Fails if the version guard is removed.
func TestIdemRerunCompletedJobIsNoOp(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	cfg := testConfig()

	startWorker(t, newWorker(t, NewRepository(pool), cfg))
	waitFor(t, 30*time.Second, "job completed", statusIs(t, pool, jobID, "completed"))
	assertCompleted(t, pool, jobID)

	// After the job, users move 100 deals back to where they came from.
	movedBack := oppIDs(t, pool, s2)[:100]
	for _, id := range movedBack {
		if err := manualMove(ctx, pool, ws, id, s1); err != nil {
			t.Fatal(err)
		}
	}
	versionsBefore := count(t, pool, `SELECT sum(version) FROM opportunities`)

	// Rewind as if the job had never run (e.g. a botched manual recovery).
	if _, err := pool.Exec(ctx, `
		UPDATE bulk_jobs SET status = 'pending', cursor_seq = 0, moved = 0, skipped = 0,
		       lease_token = NULL, lease_expires_at = NULL, finished_at = NULL
		WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "rerun finished", func() bool {
		s := loadJob(t, pool, jobID).Status
		return s == "completed" || s == "failed"
	})

	j := loadJob(t, pool, jobID)
	if j.Status != "completed" {
		// Without the version guard the rerun tries to move the deals users
		// moved back; the stage_transitions_bulk_once index (the backstop)
		// rejects the second transition and the chunk fails until the job does.
		t.Fatalf("rerun status = %s (last_error: %v), want completed", j.Status, deref(j.LastError))
	}
	if j.Moved != 0 || j.Skipped != 1000 || j.Cursor != 1000 {
		t.Fatalf("rerun: moved=%d skipped=%d cursor=%d, want 0/1000/1000", j.Moved, j.Skipped, j.Cursor)
	}
	if n := bulkTransitions(t, pool, jobID); n != 1000 {
		t.Fatalf("bulk transitions = %d, want still 1000", n)
	}
	if v := count(t, pool, `SELECT sum(version) FROM opportunities`); v != versionsBefore {
		t.Fatalf("rerun wrote to deals: version sum %d → %d", versionsBefore, v)
	}
	if n := count(t, pool, `SELECT count(*) FROM opportunities WHERE id = ANY($1) AND stage_id = $2`, movedBack, s1); n != 100 {
		t.Fatalf("rerun overrode users: only %d of 100 moved-back deals still in their stage", n)
	}
}

// 13. A duplicate bulk transition for the same job and deal is rejected by
// stage_transitions_bulk_once, the backstop behind chunk atomicity.
func TestIdemDuplicateBulkTransitionRejected(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1)
	jobID := submitJob(t, pool, ws, "k", Filter{StageID: &s1}, s2)
	opp := oppIDs(t, pool, s1)[0]

	insert := func(source string, job *int64) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO stage_transitions (workspace_id, opportunity_id, from_stage_id, to_stage_id, source, bulk_job_id)
			VALUES ($1, $2, $3, $4, $5, $6)`, ws, opp, s1, s2, source, job)
		return err
	}
	if err := insert("bulk", &jobID); err != nil {
		t.Fatal(err)
	}
	err := insert("bulk", &jobID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "stage_transitions_bulk_once" {
		t.Fatalf("duplicate bulk transition: err = %v, want unique violation on stage_transitions_bulk_once", err)
	}
	// Manual transitions (bulk_job_id NULL) are not constrained.
	for range 2 {
		if err := insert("manual", nil); err != nil {
			t.Fatalf("manual transition rejected: %v", err)
		}
	}
}

// 14. Manual move to the deal's current stage, twice → covered in the
// opportunity package: TestMoveToCurrentStageTwiceWritesNothing.
