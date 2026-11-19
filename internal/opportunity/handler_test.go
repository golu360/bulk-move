package opportunity_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/opportunity"
	"opps/internal/platform"
	"opps/internal/platform/testdb"
	"opps/internal/stage"
)

// newApp builds the app exactly like cmd/api does.
func newApp(pool *pgxpool.Pool) *fiber.App {
	app := platform.NewApp(slog.New(slog.NewTextHandler(io.Discard, nil)))
	app.Get("/healthz", platform.HealthHandler(pool))
	ws := app.Group("", platform.RequireWorkspace)
	opportunity.NewHandler(opportunity.NewService(opportunity.NewRepository(pool))).Register(ws)
	return app
}

// seedWorkspace inserts a workspace with one stage and returns their ids.
func seedWorkspace(t *testing.T, pool *pgxpool.Pool, name string) (wsID, stageID int64) {
	t.Helper()
	ctx := context.Background()
	if err := pool.QueryRow(ctx, `INSERT INTO workspaces (name) VALUES ($1) RETURNING id`, name).Scan(&wsID); err != nil {
		t.Fatal(err)
	}
	stageID, err := stage.Create(ctx, pool, wsID, 1, "New Lead")
	if err != nil {
		t.Fatal(err)
	}
	return wsID, stageID
}

type errBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func TestCreateOpportunityHandler(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	wsA, stageA := seedWorkspace(t, pool, "A")
	_, stageB := seedWorkspace(t, pool, "B")
	ws := fmt.Sprint(wsA)

	valid := fmt.Sprintf(`{"name":"Acme renewal","value_cents":1250000,"stage_id":%d,"owner_id":7}`, stageA)

	cases := []struct {
		name       string
		workspace  string // "" = header omitted
		body       string
		wantStatus int
		wantCode   string
	}{
		{"created", ws, valid, 201, ""},
		{"created with explicit status", ws,
			fmt.Sprintf(`{"name":"x","value_cents":0,"stage_id":%d,"owner_id":1,"status":"won"}`, stageA), 201, ""},
		{"empty name", ws,
			fmt.Sprintf(`{"name":"","value_cents":1,"stage_id":%d,"owner_id":7}`, stageA), 422, "validation_failed"},
		{"name too long", ws,
			fmt.Sprintf(`{"name":"%s","value_cents":1,"stage_id":%d,"owner_id":7}`, strings.Repeat("a", 201), stageA), 422, "validation_failed"},
		{"negative value", ws,
			fmt.Sprintf(`{"name":"x","value_cents":-1,"stage_id":%d,"owner_id":7}`, stageA), 422, "validation_failed"},
		{"owner zero", ws,
			fmt.Sprintf(`{"name":"x","value_cents":1,"stage_id":%d,"owner_id":0}`, stageA), 422, "validation_failed"},
		{"bad status", ws,
			fmt.Sprintf(`{"name":"x","value_cents":1,"stage_id":%d,"owner_id":7,"status":"maybe"}`, stageA), 422, "validation_failed"},
		{"stage from other workspace", ws,
			fmt.Sprintf(`{"name":"x","value_cents":1,"stage_id":%d,"owner_id":7}`, stageB), 422, "stage_not_found"},
		{"stage does not exist", ws,
			`{"name":"x","value_cents":1,"stage_id":999999,"owner_id":7}`, 422, "stage_not_found"},
		{"missing workspace header", "", valid, 400, "invalid_workspace_header"},
		{"non-numeric workspace header", "abc", valid, 400, "invalid_workspace_header"},
		{"zero workspace header", "0", valid, 400, "invalid_workspace_header"},
		{"malformed json", ws, `{"name":`, 400, "invalid_json"},
		{"unknown field", ws,
			fmt.Sprintf(`{"name":"x","value_cents":1,"stage_id":%d,"owner_id":7,"color":"red"}`, stageA), 400, "invalid_json"},
		{"wrong type", ws, `{"name":"x","value_cents":"lots","stage_id":1,"owner_id":7}`, 400, "invalid_json"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/opportunities", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			if tc.workspace != "" {
				req.Header.Set("X-Workspace-Id", tc.workspace)
			}
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, tc.wantStatus, raw)
			}
			if tc.wantCode != "" {
				var eb errBody
				if err := json.Unmarshal(raw, &eb); err != nil {
					t.Fatalf("decode error body: %v; body=%s", err, raw)
				}
				if eb.Error.Code != tc.wantCode {
					t.Fatalf("code = %q, want %q; body=%s", eb.Error.Code, tc.wantCode, raw)
				}
				return
			}

			var o opportunity.Opportunity
			if err := json.Unmarshal(raw, &o); err != nil {
				t.Fatalf("decode opportunity: %v", err)
			}
			if o.ID <= 0 || o.StageID != stageA || o.Version != 0 {
				t.Fatalf("unexpected opportunity: %+v", o)
			}
			if got, want := resp.Header.Get("Location"), fmt.Sprintf("/opportunities/%d", o.ID); got != want {
				t.Fatalf("Location = %q, want %q", got, want)
			}
		})
	}
}

func TestCreateDefaultsStatusOpenAndPersists(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	wsID, stageID := seedWorkspace(t, pool, "A")

	body := fmt.Sprintf(`{"name":"Acme","value_cents":500,"stage_id":%d,"owner_id":3}`, stageID)
	req := httptest.NewRequest(http.MethodPost, "/opportunities", strings.NewReader(body))
	req.Header.Set("X-Workspace-Id", fmt.Sprint(wsID))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var o opportunity.Opportunity
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
		t.Fatal(err)
	}
	if o.CreatedAt.Location() != time.UTC {
		t.Fatalf("created_at not UTC: %v", o.CreatedAt)
	}
	if o.Status != "open" {
		t.Fatalf("status = %q, want open", o.Status)
	}

	var gotWS int64
	var name string
	if err := pool.QueryRow(context.Background(),
		`SELECT workspace_id, name FROM opportunities WHERE id = $1`, o.ID).Scan(&gotWS, &name); err != nil {
		t.Fatal(err)
	}
	if gotWS != wsID || name != "Acme" {
		t.Fatalf("persisted row = (%d, %q), want (%d, Acme)", gotWS, name, wsID)
	}
}

// app.Test's in-memory conn surfaces fasthttp's body-limit rejection as a
// client error, so the 413 is checked through a real listener. fasthttp
// rejects on Content-Length alone and closes the connection, so we send only
// the headers: uploading the body races the close (connection reset).
func TestCreateBodyTooLarge(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() { _ = app.Shutdown() })

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintf(conn, "POST /opportunities HTTP/1.1\r\nHost: test\r\n"+
		"X-Workspace-Id: 1\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
		(1<<20)+1); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var eb errBody
	_ = json.Unmarshal(raw, &eb)
	if resp.StatusCode != 413 || eb.Error.Code != "body_too_large" {
		t.Fatalf("got %d %q, want 413 body_too_large; body=%s", resp.StatusCode, eb.Error.Code, raw)
	}
}

func TestHealthzNeedsNoWorkspaceHeader(t *testing.T) {
	pool := testdb.New(t)
	resp, err := newApp(pool).Test(httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}
}

// insertOpps bulk-inserts n opportunities into the stage and returns their ids.
func insertOpps(t *testing.T, pool *pgxpool.Pool, wsID, stageID int64, n int) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, owner_id)
		SELECT $1, $2, 'deal ' || g, g * 100, 1 FROM generate_series(1, $3) g
		RETURNING id`, wsID, stageID, n)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

type listResp struct {
	Items     []opportunity.Opportunity `json:"items"`
	NextAfter *int64                    `json:"next_after"`
}

func get(t *testing.T, app *fiber.App, path, workspace string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if workspace != "" {
		req.Header.Set("X-Workspace-Id", workspace)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func TestListStageHandler(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	wsA, stageA := seedWorkspace(t, pool, "A")
	_, stageB := seedWorkspace(t, pool, "B")
	insertOpps(t, pool, wsA, stageA, 3)
	ws := fmt.Sprint(wsA)
	base := fmt.Sprintf("/stages/%d/opportunities", stageA)

	cases := []struct {
		name       string
		workspace  string
		path       string
		wantStatus int
		wantCode   string
		wantItems  int
	}{
		{"default page", ws, base, 200, "", 3},
		{"limit 2", ws, base + "?limit=2", 200, "", 2},
		{"limit 200", ws, base + "?limit=200", 200, "", 3},
		{"limit 0", ws, base + "?limit=0", 400, "invalid_query", 0},
		{"limit 201", ws, base + "?limit=201", 400, "invalid_query", 0},
		{"limit not int", ws, base + "?limit=ten", 400, "invalid_query", 0},
		{"after not int", ws, base + "?after=x", 400, "invalid_query", 0},
		{"after negative", ws, base + "?after=-1", 400, "invalid_query", 0},
		{"stage of other workspace", ws, fmt.Sprintf("/stages/%d/opportunities", stageB), 404, "stage_not_found", 0},
		{"stage does not exist", ws, "/stages/999999/opportunities", 404, "stage_not_found", 0},
		{"bad stage id", ws, "/stages/abc/opportunities", 400, "invalid_path_param", 0},
		{"zero stage id", ws, "/stages/0/opportunities", 400, "invalid_path_param", 0},
		{"missing workspace header", "", base, 400, "invalid_workspace_header", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := get(t, app, tc.path, tc.workspace)
			if status != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", status, tc.wantStatus, raw)
			}
			if tc.wantCode != "" {
				var eb errBody
				_ = json.Unmarshal(raw, &eb)
				if eb.Error.Code != tc.wantCode {
					t.Fatalf("code = %q, want %q; body=%s", eb.Error.Code, tc.wantCode, raw)
				}
				return
			}
			var lr listResp
			if err := json.Unmarshal(raw, &lr); err != nil {
				t.Fatal(err)
			}
			if len(lr.Items) != tc.wantItems {
				t.Fatalf("items = %d, want %d", len(lr.Items), tc.wantItems)
			}
		})
	}
}

func TestListStageEmptyReturnsEmptyArray(t *testing.T) {
	pool := testdb.New(t)
	wsID, stageID := seedWorkspace(t, pool, "A")
	status, raw := get(t, newApp(pool), fmt.Sprintf("/stages/%d/opportunities", stageID), fmt.Sprint(wsID))
	if status != 200 || string(raw) != `{"items":[],"next_after":null}` {
		t.Fatalf("got %d %s", status, raw)
	}
}

// Every row in the stage appears exactly once across pages, in id order,
// and next_after is null only on the last page — including when the row
// count is an exact multiple of the page size.
func TestListStagePaginationCoversEveryRowOnce(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	wsID, stageID := seedWorkspace(t, pool, "A")
	ws := fmt.Sprint(wsID)

	// Noise: another stage in the same workspace, and another workspace.
	otherStage := addStage(t, pool, wsID, 2)
	insertOpps(t, pool, wsID, otherStage, 7)
	wsB, stageB := seedWorkspace(t, pool, "B")
	insertOpps(t, pool, wsB, stageB, 7)

	for _, tc := range []struct{ rows, limit, wantPages int }{
		{25, 10, 3},
		{30, 10, 3}, // exact multiple: no trailing empty page
		{0, 10, 1},
	} {
		t.Run(fmt.Sprintf("rows=%d limit=%d", tc.rows, tc.limit), func(t *testing.T) {
			if _, err := pool.Exec(context.Background(),
				`DELETE FROM opportunities WHERE stage_id = $1`, stageID); err != nil {
				t.Fatal(err)
			}
			want := insertOpps(t, pool, wsID, stageID, tc.rows)

			var got []int64
			pages := 0
			path := fmt.Sprintf("/stages/%d/opportunities?limit=%d", stageID, tc.limit)
			for {
				pages++
				status, raw := get(t, app, path, ws)
				if status != 200 {
					t.Fatalf("status %d: %s", status, raw)
				}
				var lr listResp
				if err := json.Unmarshal(raw, &lr); err != nil {
					t.Fatal(err)
				}
				for _, o := range lr.Items {
					got = append(got, o.ID)
				}
				if lr.NextAfter == nil {
					break
				}
				path = fmt.Sprintf("/stages/%d/opportunities?limit=%d&after=%d", stageID, tc.limit, *lr.NextAfter)
				if pages > 100 {
					t.Fatal("pagination did not terminate")
				}
			}
			if pages != tc.wantPages {
				t.Fatalf("pages = %d, want %d", pages, tc.wantPages)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("ids mismatch:\n got %v\nwant %v", got, want)
			}
		})
	}
}

func postJSON(t *testing.T, app *fiber.App, path, workspace, body string) (*http.Response, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if workspace != "" {
		req.Header.Set("X-Workspace-Id", workspace)
	}
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp, raw
}

func TestMoveHandler(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	wsA, stageA := seedWorkspace(t, pool, "A")
	stageA2 := addStage(t, pool, wsA, 2)
	wsB, stageB := seedWorkspace(t, pool, "B")
	oppA := insertOpps(t, pool, wsA, stageA, 1)[0]
	oppB := insertOpps(t, pool, wsB, stageB, 1)[0]
	ws := fmt.Sprint(wsA)
	path := func(id int64) string { return fmt.Sprintf("/opportunities/%d/move", id) }
	body := func(stage int64) string { return fmt.Sprintf(`{"stage_id":%d}`, stage) }

	cases := []struct {
		name        string
		workspace   string
		path, body  string
		wantStatus  int
		wantCode    string
		wantVersion int
	}{
		// Order matters: the first case moves oppA, the second is then a no-op.
		{"moved", ws, path(oppA), body(stageA2), 200, "", 1},
		{"already in stage", ws, path(oppA), body(stageA2), 200, "", 1},
		{"other workspace's opportunity", ws, path(oppB), body(stageA), 404, "opportunity_not_found", 0},
		{"missing opportunity", ws, path(999999), body(stageA), 404, "opportunity_not_found", 0},
		{"other workspace's stage", ws, path(oppA), body(stageB), 422, "stage_not_found", 0},
		{"missing stage_id", ws, path(oppA), `{}`, 422, "stage_not_found", 0},
		{"bad path id", ws, "/opportunities/abc/move", body(stageA), 400, "invalid_path_param", 0},
		{"zero path id", ws, "/opportunities/0/move", body(stageA), 400, "invalid_path_param", 0},
		{"malformed json", ws, path(oppA), `{"stage_id":`, 400, "invalid_json", 0},
		{"unknown field", ws, path(oppA), `{"stage_id":1,"x":1}`, 400, "invalid_json", 0},
		{"wrong type", ws, path(oppA), `{"stage_id":"one"}`, 400, "invalid_json", 0},
		{"missing workspace header", "", path(oppA), body(stageA), 400, "invalid_workspace_header", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, raw := postJSON(t, app, tc.path, tc.workspace, tc.body)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, tc.wantStatus, raw)
			}
			if tc.wantCode != "" {
				var eb errBody
				_ = json.Unmarshal(raw, &eb)
				if eb.Error.Code != tc.wantCode {
					t.Fatalf("code = %q, want %q; body=%s", eb.Error.Code, tc.wantCode, raw)
				}
				return
			}
			var o opportunity.Opportunity
			if err := json.Unmarshal(raw, &o); err != nil {
				t.Fatal(err)
			}
			if o.StageID != stageA2 || o.Version != tc.wantVersion {
				t.Fatalf("got stage=%d version=%d, want stage=%d version=%d",
					o.StageID, o.Version, stageA2, tc.wantVersion)
			}
		})
	}
}

// A row lock held longer than lock_timeout (2s) yields 503 busy + Retry-After.
func TestMoveBusyWhenRowLocked(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	ctx := context.Background()
	wsID, from := seedWorkspace(t, pool, "A")
	to := addStage(t, pool, wsID, 2)
	id := insertOpps(t, pool, wsID, from, 1)[0]

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM opportunities WHERE id = $1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	resp, raw := postJSON(t, app, fmt.Sprintf("/opportunities/%d/move", id), fmt.Sprint(wsID),
		fmt.Sprintf(`{"stage_id":%d}`, to))
	elapsed := time.Since(start)

	var eb errBody
	_ = json.Unmarshal(raw, &eb)
	if resp.StatusCode != 503 || eb.Error.Code != "busy" {
		t.Fatalf("got %d %q, want 503 busy; body=%s", resp.StatusCode, eb.Error.Code, raw)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	if elapsed < 1500*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("waited %v, expected ~2s lock_timeout", elapsed)
	}
	if n := len(transitions(t, pool, id)); n != 0 {
		t.Fatalf("transitions = %d, want 0", n)
	}
}

// Manual move to the deal's CURRENT stage, twice: 200 both times, the deal
// is returned unchanged, and no transition or version bump is written.
func TestMoveToCurrentStageTwiceWritesNothing(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	wsID, stageID := seedWorkspace(t, pool, "A")
	id := insertOpps(t, pool, wsID, stageID, 1)[0]

	for i := range 2 {
		resp, raw := postJSON(t, app, fmt.Sprintf("/opportunities/%d/move", id), fmt.Sprint(wsID),
			fmt.Sprintf(`{"stage_id":%d}`, stageID))
		if resp.StatusCode != 200 {
			t.Fatalf("move %d: status %d; body=%s", i+1, resp.StatusCode, raw)
		}
		var o opportunity.Opportunity
		if err := json.Unmarshal(raw, &o); err != nil {
			t.Fatal(err)
		}
		if o.StageID != stageID || o.Version != 0 || !o.UpdatedAt.Equal(o.CreatedAt) {
			t.Fatalf("move %d changed the deal: %+v", i+1, o)
		}
	}
	if n := len(transitions(t, pool, id)); n != 0 {
		t.Fatalf("transitions = %d, want 0", n)
	}
}
