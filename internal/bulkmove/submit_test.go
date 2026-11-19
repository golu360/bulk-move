package bulkmove

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/platform/testdb"
	"opps/internal/stage"
)

func ptr[T any](v T) *T { return &v }

// seedWorkspace inserts a workspace with two stages and returns their ids.
func seedWorkspace(t testing.TB, pool *pgxpool.Pool, name string) (wsID, stage1, stage2 int64) {
	t.Helper()
	ctx := context.Background()
	if err := pool.QueryRow(ctx, `INSERT INTO workspaces (name) VALUES ($1) RETURNING id`, name).Scan(&wsID); err != nil {
		t.Fatal(err)
	}
	for i, dst := range []*int64{&stage1, &stage2} {
		id, err := stage.Create(ctx, pool, wsID, i+1, "S")
		if err != nil {
			t.Fatal(err)
		}
		*dst = id
	}
	return wsID, stage1, stage2
}

// insertOpps inserts n open deals owned by owner 1 into the stage.
func insertOpps(t testing.TB, pool *pgxpool.Pool, wsID, stageID int64, n int) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, owner_id)
		SELECT $1, $2, 'deal ' || g, g, 1 FROM generate_series(1, $3) g`,
		wsID, stageID, n); err != nil {
		t.Fatal(err)
	}
}

func count(t testing.TB, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func newService(pool *pgxpool.Pool) *Service { return NewService(NewRepository(pool)) }

// Fails without the unique index / ON CONFLICT: concurrent retries would
// each create a job.
func TestSubmitConcurrentSameKeyCreatesOneJob(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 1000)
	svc := newService(pool)
	req := Request{Filter: Filter{StageID: &s1}, TargetStageID: s2}

	const n = 10
	var wg sync.WaitGroup
	ids := make([]int64, n)
	created := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, c, err := svc.Submit(context.Background(), ws, "k1", req)
			ids[i], created[i], errs[i] = job.ID, c, err
		}()
	}
	wg.Wait()

	nCreated := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("submit %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("submit %d got job %d, want %d", i, ids[i], ids[0])
		}
		if created[i] {
			nCreated++
		}
	}
	if nCreated != 1 {
		t.Fatalf("created=true on %d submits, want 1", nCreated)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_jobs`); got != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", got)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_job_items WHERE job_id = $1`, ids[0]); got != 1000 {
		t.Fatalf("items = %d, want 1000", got)
	}
	if got := count(t, pool, `SELECT total FROM bulk_jobs WHERE id = $1`, ids[0]); got != 1000 {
		t.Fatalf("total = %d, want 1000", got)
	}
}

func TestSubmitSameKeyDifferentBodyIsRejected(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 10)
	svc := newService(pool)
	ctx := context.Background()

	if _, _, err := svc.Submit(ctx, ws, "k", Request{Filter: Filter{StageID: &s1}, TargetStageID: s2}); err != nil {
		t.Fatal(err)
	}
	_, _, err := svc.Submit(ctx, ws, "k", Request{Filter: Filter{OwnerID: ptr(int64(1))}, TargetStageID: s2})
	if !errors.Is(err, ErrKeyReused) {
		t.Fatalf("err = %v, want ErrKeyReused", err)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_jobs`); got != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", got)
	}
}

// Keys are per workspace: the same key in another workspace is a new job.
func TestSubmitKeyIsScopedToWorkspace(t *testing.T) {
	pool := testdb.New(t)
	wsA, a1, a2 := seedWorkspace(t, pool, "A")
	wsB, b1, b2 := seedWorkspace(t, pool, "B")
	svc := newService(pool)
	ctx := context.Background()

	jA, cA, err := svc.Submit(ctx, wsA, "k", Request{Filter: Filter{StageID: &a1}, TargetStageID: a2})
	if err != nil {
		t.Fatal(err)
	}
	jB, cB, err := svc.Submit(ctx, wsB, "k", Request{Filter: Filter{StageID: &b1}, TargetStageID: b2})
	if err != nil {
		t.Fatal(err)
	}
	if !cA || !cB || jA.ID == jB.ID {
		t.Fatalf("want two new jobs, got %+v(%v) %+v(%v)", jA, cA, jB, cB)
	}
}

// The hash covers the normalized request, not the raw bytes: the same
// instant written with a different UTC offset is a replay.
func TestSubmitReplayIgnoresTimeZoneOffset(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	svc := newService(pool)
	ctx := context.Background()

	utc := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	plus2 := utc.In(time.FixedZone("+02", 2*3600))

	j1, _, err := svc.Submit(ctx, ws, "k", Request{Filter: Filter{CreatedFrom: &utc}, TargetStageID: s2})
	if err != nil {
		t.Fatal(err)
	}
	j2, created, err := svc.Submit(ctx, ws, "k", Request{Filter: Filter{CreatedFrom: &plus2}, TargetStageID: s2})
	if err != nil {
		t.Fatalf("replay with offset: %v", err)
	}
	if created || j2.ID != j1.ID {
		t.Fatalf("want replay of %d, got %d created=%v", j1.ID, j2.ID, created)
	}
	_ = s1
}

// Fails if the cap check is removed or if the job insert is not rolled back
// with the items.
func TestSubmitTooManyCreatesNothing(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, MaxItems+1)
	svc := newService(pool)

	_, _, err := svc.Submit(context.Background(), ws, "k", Request{TargetStageID: s2})
	if !errors.Is(err, ErrTooMany) {
		t.Fatalf("err = %v, want ErrTooMany", err)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_jobs`); got != 0 {
		t.Fatalf("bulk_jobs rows = %d, want 0", got)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_job_items`); got != 0 {
		t.Fatalf("bulk_job_items rows = %d, want 0", got)
	}

	// Exactly MaxItems is allowed.
	if _, err := pool.Exec(context.Background(),
		`DELETE FROM opportunities WHERE id = (SELECT max(id) FROM opportunities)`); err != nil {
		t.Fatal(err)
	}
	job, _, err := svc.Submit(context.Background(), ws, "k", Request{TargetStageID: s2})
	if err != nil || job.Total != MaxItems {
		t.Fatalf("at the cap: job=%+v err=%v, want total %d", job, err, MaxItems)
	}
}

func TestSubmitTargetStageFromOtherWorkspace(t *testing.T) {
	pool := testdb.New(t)
	wsA, _, _ := seedWorkspace(t, pool, "A")
	_, _, b2 := seedWorkspace(t, pool, "B")

	_, _, err := newService(pool).Submit(context.Background(), wsA, "k", Request{TargetStageID: b2})
	if !errors.Is(err, ErrBadStage) {
		t.Fatalf("err = %v, want ErrBadStage", err)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_jobs`); got != 0 {
		t.Fatalf("bulk_jobs rows = %d, want 0", got)
	}
}

// The snapshot is fixed at submit: contiguous seq 1..total, versions
// captured, and deals created afterwards are not included.
func TestSubmitSnapshotIsFixed(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 100)
	// A deal edited before submit: its current version is what gets captured.
	if _, err := pool.Exec(ctx, `UPDATE opportunities SET version = 7 WHERE id = (SELECT min(id) FROM opportunities)`); err != nil {
		t.Fatal(err)
	}

	job, _, err := newService(pool).Submit(ctx, ws, "k", Request{Filter: Filter{StageID: &s1}, TargetStageID: s2})
	if err != nil {
		t.Fatal(err)
	}
	insertOpps(t, pool, ws, s1, 5) // after submit

	if got := count(t, pool, `SELECT count(*) FROM bulk_job_items WHERE job_id = $1`, job.ID); got != 100 {
		t.Fatalf("items = %d, want 100", got)
	}
	if got := count(t, pool, `
		SELECT count(*) FROM bulk_job_items i JOIN opportunities o ON o.id = i.opportunity_id
		WHERE i.job_id = $1 AND i.snapshot_version = o.version`, job.ID); got != 100 {
		t.Fatalf("items with matching snapshot_version = %d, want 100", got)
	}
	// seq is 1..total, contiguous, in opportunity id order.
	if got := count(t, pool, `
		SELECT count(*) FROM (
		  SELECT seq, row_number() OVER (ORDER BY opportunity_id) AS rn
		  FROM bulk_job_items WHERE job_id = $1) x
		WHERE seq = rn`, job.ID); got != 100 {
		t.Fatalf("contiguous seq rows = %d, want 100", got)
	}
}

func TestSubmitZeroMatchesCreatesEmptyJob(t *testing.T) {
	pool := testdb.New(t)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	job, created, err := newService(pool).Submit(context.Background(), ws, "k",
		Request{Filter: Filter{StageID: &s1}, TargetStageID: s2})
	if err != nil || !created || job.Total != 0 || job.Status != "pending" {
		t.Fatalf("job=%+v created=%v err=%v, want new pending job with total 0", job, created, err)
	}
}

// Each filter field narrows the snapshot to exactly the rows SQL would pick.
func TestSubmitFilterSelectsMatchingDeals(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	wsB, b1, _ := seedWorkspace(t, pool, "B")
	insertOpps(t, pool, wsB, b1, 50) // other workspace: never matched
	if _, err := pool.Exec(ctx, `
		INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, owner_id, status, created_at)
		SELECT $1, CASE WHEN g % 2 = 0 THEN $2::bigint ELSE $3::bigint END, 'd', g * 1000, 1 + g % 3,
		       (ARRAY['open','won','lost','abandoned'])[1 + g % 4],
		       '2025-01-01T00:00:00Z'::timestamptz + make_interval(days => g)
		FROM generate_series(1, 400) g`, ws, s1, s2); err != nil {
		t.Fatal(err)
	}

	from := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)
	f := Filter{
		StageID: &s1, OwnerID: ptr(int64(2)), Status: ptr("open"),
		MinValueCents: ptr(int64(50_000)), MaxValueCents: ptr(int64(300_000)),
		CreatedFrom: &from, CreatedTo: &to,
	}
	want := count(t, pool, `
		SELECT count(*) FROM opportunities
		WHERE workspace_id = $1 AND stage_id = $2 AND owner_id = 2 AND status = 'open'
		  AND value_cents BETWEEN 50000 AND 300000
		  AND created_at >= $3 AND created_at < $4`, ws, s1, from, to)
	if want == 0 {
		t.Fatal("test data matches nothing; fix the fixture")
	}

	job, _, err := newService(pool).Submit(ctx, ws, "k", Request{Filter: f, TargetStageID: s2})
	if err != nil {
		t.Fatal(err)
	}
	if job.Total != want {
		t.Fatalf("total = %d, want %d", job.Total, want)
	}
}

func TestFilterValidate(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	cases := []struct {
		name string
		f    Filter
		ok   bool
	}{
		{"empty", Filter{}, true},
		{"full", Filter{StageID: ptr(int64(1)), OwnerID: ptr(int64(1)), Status: ptr("won"),
			MinValueCents: ptr(int64(0)), MaxValueCents: ptr(int64(0)), CreatedFrom: &t1, CreatedTo: &t2}, true},
		{"bad status", Filter{Status: ptr("maybe")}, false},
		{"sql in status", Filter{Status: ptr("open' OR 1=1 --")}, false},
		{"negative min", Filter{MinValueCents: ptr(int64(-1))}, false},
		{"negative max", Filter{MaxValueCents: ptr(int64(-1))}, false},
		{"min > max", Filter{MinValueCents: ptr(int64(2)), MaxValueCents: ptr(int64(1))}, false},
		{"from == to", Filter{CreatedFrom: &t1, CreatedTo: &t1}, false},
		{"from > to", Filter{CreatedFrom: &t2, CreatedTo: &t1}, false},
		{"stage zero", Filter{StageID: ptr(int64(0))}, false},
		{"owner negative", Filter{OwnerID: ptr(int64(-3))}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.f.Validate()
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidFilter) {
				t.Fatalf("err = %v, want ErrInvalidFilter", err)
			}
		})
	}
}

// Filter.conditions must emit only fixed @filter_* placeholders for values:
// no value from the filter may ever appear in the SQL text.
func TestFilterConditionsUseOnlyPlaceholders(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	f := Filter{
		StageID: ptr(int64(31337)), OwnerID: ptr(int64(4242)), Status: ptr("open' OR 1=1 --"),
		MinValueCents: ptr(int64(777)), MaxValueCents: ptr(int64(888)), CreatedFrom: &t1, CreatedTo: &t2,
	}
	args := pgx.NamedArgs{"workspace_id": int64(1)}
	sql := f.conditions(args)

	want := " AND stage_id = @filter_stage_id AND owner_id = @filter_owner_id AND status = @filter_status" +
		" AND value_cents >= @filter_min_value_cents AND value_cents <= @filter_max_value_cents" +
		" AND created_at >= @filter_created_from AND created_at < @filter_created_to"
	if sql != want {
		t.Fatalf("sql =\n%q\nwant\n%q", sql, want)
	}
	wantArgs := pgx.NamedArgs{
		"workspace_id":           int64(1),
		"filter_stage_id":        int64(31337),
		"filter_owner_id":        int64(4242),
		"filter_status":          "open' OR 1=1 --",
		"filter_min_value_cents": int64(777),
		"filter_max_value_cents": int64(888),
		"filter_created_from":    t1,
		"filter_created_to":      t2,
	}
	if fmt.Sprint(args) != fmt.Sprint(wantArgs) {
		t.Fatalf("args =\n%v\nwant\n%v", args, wantArgs)
	}
	// Belt and braces: nothing but identifiers, operators and @params.
	if !regexp.MustCompile(`^( AND [a-z_]+ (=|>=|<=|<) @filter_[a-z_]+)*$`).MatchString(sql) {
		t.Fatalf("unexpected SQL shape: %q", sql)
	}
	for _, v := range []string{"31337", "4242", "OR 1=1", "777", "888", "2025"} {
		if strings.Contains(sql, v) {
			t.Fatalf("value %q leaked into SQL: %s", v, sql)
		}
	}

	// Unset fields add neither a condition nor an arg.
	empty := pgx.NamedArgs{}
	if sql := (Filter{}).conditions(empty); sql != "" || len(empty) != 0 {
		t.Fatalf("empty filter: sql=%q args=%v", sql, empty)
	}
}
