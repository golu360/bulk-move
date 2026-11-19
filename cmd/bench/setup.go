package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/bulkmove"
)

const apiURL = "http://localhost:8080"

var client = &http.Client{
	Timeout:   15 * time.Second,
	Transport: &http.Transport{MaxIdleConnsPerHost: 512},
}

type bench struct {
	ctx         context.Context
	pool        *pgxpool.Pool
	wss         []workspace
	dir         string // built binaries and their logs
	api, worker *proc
}

// workspace is one seeded workspace, as the benchmark targets it.
type workspace struct {
	id             int64
	count          int
	minOpp, maxOpp int64      // seeded ids are contiguous per workspace
	stages         []int64    // stage ids by position
	createdTo      *time.Time // bulk filter: its oldest MaxItems deals; nil = all of them
}

// Every seed has 12 stages; bulk moves target the last two.
const lastStage = 11

// setup connects, loads the seeded workspaces, and starts the API and worker.
func setup(ctx context.Context) (b *bench, err error) {
	b = &bench{ctx: ctx}
	defer func() {
		if err != nil {
			b.close()
		}
	}()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	if b.pool, err = pgxpool.New(ctx, dbURL); err != nil {
		return nil, err
	}
	if err = b.preflight(); err != nil {
		return nil, err
	}
	if b.wss, err = b.loadWorkspaces(); err != nil {
		return nil, err
	}
	if b.dir, err = build("api", "worker"); err != nil {
		return nil, err
	}
	if b.api, err = b.start("api"); err != nil {
		return nil, err
	}
	if err = waitHealthy(); err != nil {
		return nil, err
	}
	if b.worker, err = b.start("worker", workerEnv...); err != nil {
		return nil, err
	}

	total := 0
	for _, ws := range b.wss {
		total += ws.count
	}
	fmt.Printf("seed: %d workspace(s), %d opportunities\nworker: %s\nlogs: %s\n",
		len(b.wss), total, strings.Join(workerEnv, " "), b.dir)
	return b, nil
}

func (b *bench) close() {
	if b.worker != nil {
		b.worker.stop()
	}
	if b.api != nil {
		b.api.stop()
	}
	if b.pool != nil {
		b.pool.Close()
	}
}

// preflight: our own API and worker must be the only ones running.
func (b *bench) preflight() error {
	if resp, err := client.Get(apiURL + "/healthz"); err == nil {
		resp.Body.Close()
		return errors.New("something is already listening on :8080; stop your API first")
	}
	var workers int
	if err := b.pool.QueryRow(b.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'bulk-worker'`).Scan(&workers); err != nil {
		return err
	}
	if workers > 0 {
		return errors.New("a bulk worker is already connected to the database; stop it first")
	}
	return nil
}

func (b *bench) loadWorkspaces() ([]workspace, error) {
	rows, _ := b.pool.Query(b.ctx, `
		SELECT workspace_id, count(*), min(id), max(id)
		FROM opportunities GROUP BY workspace_id ORDER BY workspace_id`)
	wss, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (workspace, error) {
		var ws workspace
		return ws, r.Scan(&ws.id, &ws.count, &ws.minOpp, &ws.maxOpp)
	})
	if err != nil {
		return nil, err
	}
	if len(wss) == 0 {
		return nil, errors.New("no opportunities; run make seed-large or make seed-small first")
	}

	for i := range wss {
		ws := &wss[i]
		rows, _ := b.pool.Query(b.ctx, `SELECT id FROM stages WHERE workspace_id = $1 ORDER BY position`, ws.id)
		if ws.stages, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err != nil {
			return nil, err
		}

		// created_to (exclusive) = the (MaxItems+1)th oldest deal's created_at.
		var to time.Time
		err := b.pool.QueryRow(b.ctx, `
			SELECT created_at FROM opportunities WHERE workspace_id = $1
			ORDER BY created_at OFFSET $2 LIMIT 1`, ws.id, bulkmove.MaxItems).Scan(&to)
		switch {
		case err == nil:
			ws.createdTo = &to
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, err
		}
	}
	return wss, nil
}

// ── Processes ───────────────────────────────────────────────────────────

// build compiles ./cmd/<name> for each name into a new temp dir.
func build(names ...string) (string, error) {
	dir, err := os.MkdirTemp("", "bulk-move-bench-")
	if err != nil {
		return "", err
	}
	for _, name := range names {
		out, err := exec.Command("go", "build", "-o", filepath.Join(dir, name), "./cmd/"+name).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("build %s: %v\n%s", name, err, out)
		}
	}
	return dir, nil
}

type proc struct {
	cmd    *exec.Cmd
	log    *os.File
	exited bool
}

// start runs a built binary, appending its output to <name>.log.
func (b *bench) start(name string, env ...string) (*proc, error) {
	log, err := os.OpenFile(filepath.Join(b.dir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(filepath.Join(b.dir, name))
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	return &proc{cmd: cmd, log: log}, nil
}

func (p *proc) stop() { p.signal(syscall.SIGTERM) }
func (p *proc) kill() { p.signal(syscall.SIGKILL) }

func (p *proc) signal(sig os.Signal) {
	if p.exited {
		return
	}
	p.exited = true
	_ = p.cmd.Process.Signal(sig)
	_ = p.cmd.Wait()
	p.log.Close()
}

func waitHealthy() error {
	for range 100 {
		if resp, err := client.Get(apiURL + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("API did not become healthy within 10s (see api.log)")
}
