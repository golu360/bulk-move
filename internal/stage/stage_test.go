package stage_test

import (
	"context"
	"testing"

	"opps/internal/platform/testdb"
	"opps/internal/stage"
)

func TestCreateAndExistsAreWorkspaceScoped(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	var wsA, wsB int64
	for _, p := range []struct {
		dst  *int64
		name string
	}{{&wsA, "A"}, {&wsB, "B"}} {
		if err := pool.QueryRow(ctx, `INSERT INTO workspaces (name) VALUES ($1) RETURNING id`, p.name).Scan(p.dst); err != nil {
			t.Fatal(err)
		}
	}

	sA, err := stage.Create(ctx, pool, wsA, 1, "New Lead")
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name      string
		ws, stage int64
		want      bool
	}{
		{"own stage", wsA, sA, true},
		{"other workspace's stage", wsB, sA, false},
		{"missing stage", wsA, sA + 1000, false},
	} {
		got, err := stage.Exists(ctx, pool, tc.ws, tc.stage)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%s: Exists = %v, want %v", tc.name, got, tc.want)
		}
	}

	// Works inside a caller's transaction too, and sees its uncommitted rows.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	sB, err := stage.Create(ctx, tx, wsB, 1, "New Lead")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := stage.Exists(ctx, tx, wsB, sB); err != nil || !ok {
		t.Fatalf("Exists in tx = %v, %v; want true", ok, err)
	}

	// (workspace_id, position) is unique.
	if _, err := stage.Create(ctx, pool, wsA, 1, "Duplicate"); err == nil {
		t.Fatal("duplicate position: want error")
	}
}
