package bulkmove

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/platform"
	"opps/internal/platform/testdb"
)

// newApp builds the app exactly like cmd/api does.
func newApp(pool *pgxpool.Pool) *fiber.App {
	app := platform.NewApp(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ws := app.Group("", platform.RequireWorkspace)
	NewHandler(NewService(NewRepository(pool))).Register(ws)
	return app
}

type errBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func submit(t *testing.T, app *fiber.App, workspace, key, body string) (*http.Response, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/bulk-moves", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if workspace != "" {
		req.Header.Set("X-Workspace-Id", workspace)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp, raw
}

// get sends GET path as workspace and returns the response and body.
func get(t *testing.T, app *fiber.App, workspace, path string) (*http.Response, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Workspace-Id", workspace)
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp, raw
}

func TestSubmitHandlerReplay(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	insertOpps(t, pool, ws, s1, 25)
	wsHdr := fmt.Sprint(ws)

	body := fmt.Sprintf(`{"filter":{"stage_id":%d},"target_stage_id":%d}`, s1, s2)
	// Same request, different whitespace and field order.
	body2 := fmt.Sprintf("{ \"target_stage_id\": %d,\n  \"filter\": { \"stage_id\": %d } }", s2, s1)

	r1, raw1 := submit(t, app, wsHdr, "k1", body)
	if r1.StatusCode != 202 {
		t.Fatalf("first submit: %d %s", r1.StatusCode, raw1)
	}
	if h := r1.Header.Get(ReplayedHeader); h != "" {
		t.Fatalf("first submit has %s=%q, want absent", ReplayedHeader, h)
	}
	var j1 Job
	if err := json.Unmarshal(raw1, &j1); err != nil {
		t.Fatal(err)
	}
	if j1.Status != "pending" || j1.Total != 25 {
		t.Fatalf("first job = %+v", j1)
	}
	wantLoc := fmt.Sprintf("/bulk-moves/%d", j1.ID)
	if got := r1.Header.Get("Location"); got != wantLoc {
		t.Fatalf("Location = %q, want %q", got, wantLoc)
	}

	r2, raw2 := submit(t, app, wsHdr, "k1", body2)
	if r2.StatusCode != 202 {
		t.Fatalf("replay: %d %s", r2.StatusCode, raw2)
	}
	if got := r2.Header.Get(ReplayedHeader); got != "true" {
		t.Fatalf("replay %s = %q, want true", ReplayedHeader, got)
	}
	if got := r2.Header.Get("Location"); got != wantLoc {
		t.Fatalf("replay Location = %q, want %q", got, wantLoc)
	}
	if string(raw2) != string(raw1) {
		t.Fatalf("replay body = %s, want %s", raw2, raw1)
	}
	if got := count(t, pool, `SELECT count(*) FROM bulk_jobs`); got != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", got)
	}
}

func TestSubmitHandlerErrors(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	_, b1, _ := seedWorkspace(t, pool, "B")
	big, bs1, bs2 := seedWorkspace(t, pool, "Big")
	insertOpps(t, pool, big, bs1, MaxItems+1)
	wsHdr := fmt.Sprint(ws)

	ok := fmt.Sprintf(`{"filter":{"stage_id":%d},"target_stage_id":%d}`, s1, s2)
	// Claim key "used" with the ok body so it can be reused with another.
	if r, raw := submit(t, app, wsHdr, "used", ok); r.StatusCode != 202 {
		t.Fatalf("setup submit: %d %s", r.StatusCode, raw)
	}

	cases := []struct {
		name       string
		workspace  string
		key        string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"missing key", wsHdr, "", ok, 400, "missing_idempotency_key"},
		{"key too long", wsHdr, strings.Repeat("k", 201), ok, 400, "missing_idempotency_key"},
		{"key not ascii", wsHdr, "clé", ok, 400, "missing_idempotency_key"},
		{"key reused", wsHdr, "used",
			fmt.Sprintf(`{"filter":{"owner_id":1},"target_stage_id":%d}`, s2), 422, "idempotency_key_reused"},
		{"bad status", wsHdr, "k-status",
			fmt.Sprintf(`{"filter":{"status":"maybe"},"target_stage_id":%d}`, s2), 422, "invalid_filter"},
		{"sql in status", wsHdr, "k-sqli",
			fmt.Sprintf(`{"filter":{"status":"open' OR 1=1 --"},"target_stage_id":%d}`, s2), 422, "invalid_filter"},
		{"min > max", wsHdr, "k-range",
			fmt.Sprintf(`{"filter":{"min_value_cents":5,"max_value_cents":1},"target_stage_id":%d}`, s2), 422, "invalid_filter"},
		{"negative value", wsHdr, "k-neg",
			fmt.Sprintf(`{"filter":{"min_value_cents":-1},"target_stage_id":%d}`, s2), 422, "invalid_filter"},
		{"from >= to", wsHdr, "k-dates",
			fmt.Sprintf(`{"filter":{"created_from":"2026-01-01T00:00:00Z","created_to":"2025-01-01T00:00:00Z"},"target_stage_id":%d}`, s2),
			422, "invalid_filter"},
		{"target stage in other workspace", wsHdr, "k-stage",
			fmt.Sprintf(`{"filter":{},"target_stage_id":%d}`, b1), 422, "target_stage_not_found"},
		{"target stage missing", wsHdr, "k-nostage", `{"filter":{}}`, 422, "target_stage_not_found"},
		{"too many matches", fmt.Sprint(big), "k-big",
			fmt.Sprintf(`{"filter":{},"target_stage_id":%d}`, bs2), 422, "too_many_matches"},
		{"malformed json", wsHdr, "k-json", `{"filter":`, 400, "invalid_json"},
		{"unknown field", wsHdr, "k-unknown",
			fmt.Sprintf(`{"filter":{"color":"red"},"target_stage_id":%d}`, s2), 400, "invalid_json"},
		{"bad timestamp", wsHdr, "k-ts",
			fmt.Sprintf(`{"filter":{"created_from":"yesterday"},"target_stage_id":%d}`, s2), 400, "invalid_json"},
		{"missing workspace header", "", "k", ok, 400, "invalid_workspace_header"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, raw := submit(t, app, tc.workspace, tc.key, tc.body)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, tc.wantStatus, raw)
			}
			if h := resp.Header.Get(ReplayedHeader); h != "" {
				t.Fatalf("error response carries %s=%q", ReplayedHeader, h)
			}
			var eb errBody
			if err := json.Unmarshal(raw, &eb); err != nil {
				t.Fatalf("decode: %v; body=%s", err, raw)
			}
			if eb.Error.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q; body=%s", eb.Error.Code, tc.wantCode, raw)
			}
		})
	}

	// None of the failures created a job; only the setup submit did.
	if got := count(t, pool, `SELECT count(*) FROM bulk_jobs`); got != 1 {
		t.Fatalf("bulk_jobs rows = %d, want 1", got)
	}
}

func TestSubmitHandlerZeroMatches(t *testing.T) {
	pool := testdb.New(t)
	app := newApp(pool)
	ws, s1, s2 := seedWorkspace(t, pool, "A")
	resp, raw := submit(t, app, fmt.Sprint(ws), "k",
		fmt.Sprintf(`{"filter":{"stage_id":%d},"target_stage_id":%d}`, s1, s2))
	if resp.StatusCode != 202 {
		t.Fatalf("status = %d; body=%s", resp.StatusCode, raw)
	}
	var j Job
	_ = json.Unmarshal(raw, &j)
	if j.Total != 0 || j.Status != "pending" {
		t.Fatalf("job = %+v, want pending with total 0", j)
	}
}
