// Command bench measures bulk moves against whatever seed is loaded
// (make bench-large / make bench-small seed first). It builds and runs its
// own API (:8080) and worker, so neither may already be running.
//
//  1. baseline  interactive latency with no bulk job running
//  2. bulk      one bulk move per workspace (its oldest ≤50,000 deals) with
//     interactive load alongside: wall-clock, throughput, p95/p99 latency
//  3. kill      another bulk move per workspace; the worker is SIGKILLed part
//     way and a new one started: time to completion, and SQL proof that
//     every deal was moved exactly once
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

var (
	rate        = flag.Int("rate", 200, "interactive requests per second")
	baselineFor = flag.Duration("baseline", 10*time.Second, "how long to measure latency with no bulk job")
	killAt      = flag.Float64("kill-at", 0.5, "kill the worker once this fraction of items is processed")
)

// workerEnv: a killed worker's job is resumed once its lease expires, so a
// short lease keeps the kill-and-resume wait short. The worker requires
// LEASE_TTL > LOCK_TIMEOUT + CHUNK_PAUSE, so the lock timeout comes down too.
var workerEnv = []string{"LEASE_TTL=2s", "LOCK_TIMEOUT=1s", "POLL_EVERY=100ms"}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run() error {
	b, err := setup(context.Background())
	if err != nil {
		return err
	}
	defer b.close()

	b.baseline()
	if err := b.bulkMove(); err != nil {
		return err
	}
	return b.killAndResume()
}

// baseline measures interactive latency with no bulk job running.
func (b *bench) baseline() {
	header("1/3 baseline: %d req/s for %s, no bulk job", *rate, *baselineFor)
	ctx, cancel := context.WithTimeout(b.ctx, *baselineFor)
	defer cancel()
	runLoad(ctx, b.wss, nil).print()
}

// bulkMove runs one bulk move per workspace with interactive load alongside.
func (b *bench) bulkMove() error {
	header("2/3 bulk move: %d job(s), %d req/s interactive load alongside", len(b.wss), *rate)
	start := time.Now()
	jobs, err := b.submitAll("bulk", lastStage)
	if err != nil {
		return err
	}

	ctx, stopLoad := context.WithCancel(b.ctx)
	latency := make(chan *recorder)
	go func() { latency <- runLoad(ctx, b.wss, jobs) }()

	rows, err := b.waitUntil(jobs, allFinished)
	wall := time.Since(start)
	stopLoad()
	lat := <-latency
	if err != nil {
		return err
	}

	printJobs(rows)
	fmt.Printf("  wall-clock (submit → all completed): %s\n", round(wall))
	printThroughput(rows)
	fmt.Println("  interactive latency during the run:")
	lat.print()
	return nil
}

// killAndResume SIGKILLs the worker part way through a bulk move, starts a
// new one, and proves the finished result is correct.
func (b *bench) killAndResume() error {
	header("3/3 kill and resume: %d job(s), worker SIGKILLed at %.0f%%", len(b.wss), *killAt*100)
	start := time.Now()
	jobs, err := b.submitAll("kill", lastStage-1)
	if err != nil {
		return err
	}

	// 1. Wait for the kill point, then kill.
	rows, err := b.waitUntil(jobs, func(rows []jobRow) bool {
		done, total := progress(rows)
		return float64(done) >= *killAt*float64(total)
	})
	if err != nil {
		return err
	}
	if allFinished(rows) {
		return errors.New("jobs finished before the kill point; lower -kill-at")
	}
	b.worker.kill()
	killedAt := time.Now()

	// 2. Note where each running job stopped (its in-flight chunk rolled back).
	time.Sleep(200 * time.Millisecond)
	if rows, err = b.states(jobs); err != nil {
		return err
	}
	stoppedAt := map[int64]int{}
	for _, j := range rows {
		if j.status == "running" {
			stoppedAt[j.id] = j.cursor
		}
	}
	done, total := progress(rows)
	fmt.Printf("  killed worker at %d/%d items (%.0f%%) with %d job(s) mid-run; starting a new worker\n",
		done, total, 100*float64(done)/float64(total), len(stoppedAt))

	// 3. New worker: wait for an interrupted job to move again, then for the end.
	if b.worker, err = b.start("worker", workerEnv...); err != nil {
		return err
	}
	if _, err := b.waitUntil(jobs, func(rows []jobRow) bool {
		for _, j := range rows {
			if c, ok := stoppedAt[j.id]; ok && j.cursor > c {
				return true
			}
		}
		return allFinished(rows)
	}); err != nil {
		return err
	}
	resumeGap := time.Since(killedAt)
	if rows, err = b.waitUntil(jobs, allFinished); err != nil {
		return err
	}

	fmt.Printf("  time to completion (submit → all completed): %s\n", round(time.Since(start)))
	fmt.Printf("  resume gap (kill → interrupted job moving again): %s\n", round(resumeGap))
	printJobs(rows)
	return b.verify(rows)
}

func header(format string, a ...any) {
	fmt.Printf("\n== "+format+"\n", a...)
}

func round(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
