package opportunity_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/opportunity"
	"opps/internal/platform/testdb"
	"opps/internal/stage"
)

func addStage(t *testing.T, pool *pgxpool.Pool, wsID int64, position int) int64 {
	t.Helper()
	id, err := stage.Create(context.Background(), pool, wsID, position, "Stage")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type transition struct {
	from, to int64
	source   string
	bulkJob  *int64
}

func transitions(t *testing.T, pool *pgxpool.Pool, oppID int64) []transition {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT from_stage_id, to_stage_id, source, bulk_job_id FROM stage_transitions
		 WHERE opportunity_id = $1 ORDER BY id`, oppID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []transition
	for rows.Next() {
		var tr transition
		if err := rows.Scan(&tr.from, &tr.to, &tr.source, &tr.bulkJob); err != nil {
			t.Fatal(err)
		}
		out = append(out, tr)
	}
	return out
}

func TestMoveRecordsOneManualTransitionAndBumpsVersion(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	svc := opportunity.NewService(opportunity.NewRepository(pool))
	wsID, from := seedWorkspace(t, pool, "A")
	to := addStage(t, pool, wsID, 2)
	id := insertOpps(t, pool, wsID, from, 1)[0]

	o, err := svc.Move(ctx, wsID, id, opportunity.MoveInput{StageID: to})
	if err != nil {
		t.Fatal(err)
	}
	if o.StageID != to || o.Version != 1 {
		t.Fatalf("after move: stage=%d version=%d, want stage=%d version=1", o.StageID, o.Version, to)
	}
	if !o.UpdatedAt.After(o.CreatedAt) {
		t.Fatalf("updated_at not advanced: %v vs %v", o.UpdatedAt, o.CreatedAt)
	}

	trs := transitions(t, pool, id)
	if len(trs) != 1 {
		t.Fatalf("transitions = %d, want 1", len(trs))
	}
	if tr := trs[0]; tr.from != from || tr.to != to || tr.source != "manual" || tr.bulkJob != nil {
		t.Fatalf("transition = %+v, want %d->%d manual", tr, from, to)
	}

	// Moving again to the same stage is a no-op: no write, no transition.
	o2, err := svc.Move(ctx, wsID, id, opportunity.MoveInput{StageID: to})
	if err != nil {
		t.Fatal(err)
	}
	if o2.Version != 1 || !o2.UpdatedAt.Equal(o.UpdatedAt) {
		t.Fatalf("no-op move changed the row: %+v", o2)
	}
	if n := len(transitions(t, pool, id)); n != 1 {
		t.Fatalf("transitions after no-op = %d, want 1", n)
	}
}

func TestMoveIsWorkspaceScoped(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	svc := opportunity.NewService(opportunity.NewRepository(pool))
	wsA, stageA := seedWorkspace(t, pool, "A")
	stageA2 := addStage(t, pool, wsA, 2)
	wsB, stageB := seedWorkspace(t, pool, "B")
	oppA := insertOpps(t, pool, wsA, stageA, 1)[0]
	oppB := insertOpps(t, pool, wsB, stageB, 1)[0]

	// Another workspace's opportunity id → not found.
	if _, err := svc.Move(ctx, wsA, oppB, opportunity.MoveInput{StageID: stageA2}); !errors.Is(err, opportunity.ErrNotFound) {
		t.Fatalf("move other workspace's opp: err = %v, want ErrNotFound", err)
	}
	// Another workspace's stage → stage not found.
	if _, err := svc.Move(ctx, wsA, oppA, opportunity.MoveInput{StageID: stageB}); !errors.Is(err, opportunity.ErrStageNotFound) {
		t.Fatalf("move to other workspace's stage: err = %v, want ErrStageNotFound", err)
	}

	// Neither attempt wrote anything.
	var stage int64
	var version int
	for _, id := range []int64{oppA, oppB} {
		if err := pool.QueryRow(ctx, `SELECT stage_id, version FROM opportunities WHERE id = $1`, id).
			Scan(&stage, &version); err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("opp %d version = %d, want 0", id, version)
		}
		if n := len(transitions(t, pool, id)); n != 0 {
			t.Fatalf("opp %d has %d transitions, want 0", id, n)
		}
	}
}
