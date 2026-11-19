package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	epList = "GET  /stages/:id/opportunities"
	epMove = "POST /opportunities/:id/move"
	epJob  = "GET  /bulk-moves/:id"
)

type request struct {
	endpoint, method, path, body string
	ws                           int64
}

// pick: 60% stage listings, 30% single-deal moves (random deals, so some
// collide with the bulk job), 10% job status (a listing when there is no job).
func pick(rng *rand.Rand, wss []workspace, jobOf map[int64]int64) request {
	ws := wss[rng.IntN(len(wss))]
	stage := ws.stages[rng.IntN(len(ws.stages))]
	jobID, hasJob := jobOf[ws.id]
	switch r := rng.IntN(10); {
	case r < 6 || (r == 9 && !hasJob):
		return request{epList, http.MethodGet, fmt.Sprintf("/stages/%d/opportunities?limit=50", stage), "", ws.id}
	case r < 9:
		opp := ws.minOpp + rng.Int64N(ws.maxOpp-ws.minOpp+1)
		return request{epMove, http.MethodPost, fmt.Sprintf("/opportunities/%d/move", opp),
			fmt.Sprintf(`{"stage_id":%d}`, stage), ws.id}
	default:
		return request{epJob, http.MethodGet, fmt.Sprintf("/bulk-moves/%d", jobID), "", ws.id}
	}
}

// runLoad sends *rate requests per second until ctx ends. It is open loop
// (a slow response never delays the next send) and measures latency from
// the scheduled send time, so a stalled server can't hide its tail.
func runLoad(ctx context.Context, wss []workspace, jobs []job) *recorder {
	jobOf := map[int64]int64{}
	for _, j := range jobs {
		jobOf[j.ws] = j.id
	}
	rec := &recorder{latency: map[string][]time.Duration{}, failed: map[string]map[int]int{}}
	rng := rand.New(rand.NewPCG(1, 2))
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return rec
		case scheduled := <-tick.C:
			r := pick(rng, wss, jobOf)
			wg.Go(func() { rec.send(r, scheduled) })
		}
	}
}

type recorder struct {
	mu      sync.Mutex
	latency map[string][]time.Duration
	failed  map[string]map[int]int // endpoint → non-2xx status → count (0 = no response)
}

func (rec *recorder) send(r request, scheduled time.Time) {
	req, _ := http.NewRequest(r.method, apiURL+r.path, strings.NewReader(r.body))
	req.Header.Set("X-Workspace-Id", fmt.Sprint(r.ws))
	req.Header.Set("Content-Type", "application/json")
	status := 0
	if resp, err := client.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		status = resp.StatusCode
	}
	took := time.Since(scheduled)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.latency[r.endpoint] = append(rec.latency[r.endpoint], took)
	if status < 200 || status > 299 {
		if rec.failed[r.endpoint] == nil {
			rec.failed[r.endpoint] = map[int]int{}
		}
		rec.failed[r.endpoint][status]++
	}
}

func (rec *recorder) print() {
	fmt.Printf("    %-32s %6s %8s %8s %8s %8s  %s\n", "endpoint", "count", "p50", "p95", "p99", "max", "non-2xx")
	for _, ep := range []string{epList, epMove, epJob} {
		lat := rec.latency[ep]
		if len(lat) == 0 {
			continue
		}
		slices.Sort(lat)
		var failed []string
		for status, n := range rec.failed[ep] {
			failed = append(failed, fmt.Sprintf("%d×%d", status, n))
		}
		slices.Sort(failed)
		fmt.Printf("    %-32s %6d %8s %8s %8s %8s  %s\n", ep, len(lat),
			ms(pct(lat, 50)), ms(pct(lat, 95)), ms(pct(lat, 99)), ms(lat[len(lat)-1]), strings.Join(failed, " "))
	}
}

// pct returns the p-th percentile (nearest rank) of sorted durations.
func pct(sorted []time.Duration, p int) time.Duration {
	rank := (p*len(sorted) + 99) / 100 // ceil(p/100 * n)
	return sorted[max(rank-1, 0)]
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}
