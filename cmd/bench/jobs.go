package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type job struct{ id, ws int64 }

// submitAll submits one bulk move per workspace: its oldest ≤MaxItems deals
// to the stage at targetPos.
func (b *bench) submitAll(phase string, targetPos int) ([]job, error) {
	var jobs []job
	for _, ws := range b.wss {
		filter := "{}"
		if ws.createdTo != nil {
			filter = fmt.Sprintf(`{"created_to":%q}`, ws.createdTo.Format(time.RFC3339Nano))
		}
		body := fmt.Sprintf(`{"filter":%s,"target_stage_id":%d}`, filter, ws.stages[targetPos])

		req, _ := http.NewRequest(http.MethodPost, apiURL+"/bulk-moves", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Workspace-Id", fmt.Sprint(ws.id))
		req.Header.Set("Idempotency-Key", fmt.Sprintf("bench-%s-%d-%d", phase, ws.id, time.Now().UnixNano()))
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("submit: %w", err)
		}
		var created struct {
			ID int64 `json:"id"`
		}
		err = json.NewDecoder(resp.Body).Decode(&created)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted || err != nil {
			return nil, fmt.Errorf("submit to workspace %d: HTTP %d", ws.id, resp.StatusCode)
		}
		jobs = append(jobs, job{id: created.ID, ws: ws.id})
	}
	return jobs, nil
}

// jobRow is a job's state, read straight from bulk_jobs.
type jobRow struct {
	id, ws                                int64
	status                                string
	total, cursor, moved, skipped, claims int
	startedAt, finishedAt                 *time.Time
}

func (b *bench) states(jobs []job) ([]jobRow, error) {
	ids := make([]int64, len(jobs))
	for i, j := range jobs {
		ids[i] = j.id
	}
	rows, _ := b.pool.Query(b.ctx, `
		SELECT id, workspace_id, status, total, cursor_seq, moved, skipped, claims, started_at, finished_at
		FROM bulk_jobs WHERE id = ANY($1) ORDER BY id`, ids)
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (jobRow, error) {
		var j jobRow
		return j, r.Scan(&j.id, &j.ws, &j.status, &j.total, &j.cursor, &j.moved, &j.skipped,
			&j.claims, &j.startedAt, &j.finishedAt)
	})
}

// waitUntil polls the jobs every 50ms until cond holds.
func (b *bench) waitUntil(jobs []job, cond func([]jobRow) bool) ([]jobRow, error) {
	for deadline := time.Now().Add(15 * time.Minute); time.Now().Before(deadline); {
		rows, err := b.states(jobs)
		if err != nil {
			return nil, err
		}
		if cond(rows) {
			return rows, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, errors.New("jobs did not finish within 15 minutes")
}

func allFinished(rows []jobRow) bool {
	for _, j := range rows {
		if j.status == "pending" || j.status == "running" {
			return false
		}
	}
	return true
}

// progress: items processed and total, across all jobs.
func progress(rows []jobRow) (done, total int) {
	for _, j := range rows {
		done += j.cursor
		total += j.total
	}
	return done, total
}

func printJobs(rows []jobRow) {
	for _, j := range rows {
		ran := "-"
		if j.startedAt != nil && j.finishedAt != nil {
			ran = round(j.finishedAt.Sub(*j.startedAt)).String()
		}
		fmt.Printf("  workspace %d, job %d: %s, %d items (moved %d, skipped %d), claimed %d×, ran %s\n",
			j.ws, j.id, j.status, j.total, j.moved, j.skipped, j.claims, ran)
	}
}

// printThroughput: items over the window jobs were actually running
// (first start → last finish, database clock).
func printThroughput(rows []jobRow) {
	var first, last time.Time
	var items, moved int
	for _, j := range rows {
		if j.startedAt == nil || j.finishedAt == nil {
			continue
		}
		if first.IsZero() || j.startedAt.Before(first) {
			first = *j.startedAt
		}
		if j.finishedAt.After(last) {
			last = *j.finishedAt
		}
		items += j.total
		moved += j.moved
	}
	window := last.Sub(first)
	if window <= 0 {
		return
	}
	fmt.Printf("  sustained throughput: %.0f items/s (%.0f moved/s) over %s of processing\n",
		float64(items)/window.Seconds(), float64(moved)/window.Seconds(), round(window))
}
