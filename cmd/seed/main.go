// Command seed populates a fresh database with deterministic, uneven CRM
// data for local development and benchmarking.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/stage"
)

type stageSpec struct {
	name   string
	weight float64
}

// Uneven stage distribution: most deals sit early in the pipeline.
var stageSpecs = []stageSpec{
	{"New Lead", 22},
	{"Contacted", 18},
	{"Qualified", 14},
	{"Discovery", 11},
	{"Demo Scheduled", 9},
	{"Demo Done", 7},
	{"Proposal Sent", 5},
	{"Negotiation", 4},
	{"Contract Sent", 3},
	{"Verbal Commit", 3},
	{"Closed Won", 2},
	{"Closed Lost", 2},
}

type workspaceSpec struct {
	name  string
	count int
}

func workspaceSpecs(profile string) ([]workspaceSpec, error) {
	switch profile {
	case "full":
		specs := []workspaceSpec{{"Large Workspace", 500_000}}
		for i := 1; i <= 5; i++ {
			specs = append(specs, workspaceSpec{fmt.Sprintf("Small Workspace %d", i), 3_000})
		}
		return specs, nil
	case "small":
		specs := []workspaceSpec{{"Large Workspace", 20_000}}
		for i := 1; i <= 5; i++ {
			specs = append(specs, workspaceSpec{fmt.Sprintf("Small Workspace %d", i), 1_000})
		}
		return specs, nil
	default:
		return nil, fmt.Errorf("unknown profile %q (want small|full)", profile)
	}
}

func main() {
	profile := flag.String("profile", "small", "seed profile: small|full")
	seed := flag.Uint64("seed", 42, "random seed; same seed produces identical data")
	reset := flag.Bool("reset", false, "truncate all tables before seeding")
	flag.Parse()

	specs, err := workspaceSpecs(*profile)
	if err != nil {
		log.Fatal(err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if *reset {
		const truncate = `TRUNCATE stage_transitions, bulk_job_items, bulk_jobs, opportunities, stages, workspaces RESTART IDENTITY CASCADE`
		if _, err := pool.Exec(ctx, truncate); err != nil {
			log.Fatalf("truncate: %v", err)
		}
	} else {
		var existing int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM workspaces`).Scan(&existing); err != nil {
			log.Fatalf("check existing data: %v", err)
		}
		if existing > 0 {
			log.Fatal("workspaces already exist; pass -reset to truncate first")
		}
	}

	rng := rand.New(rand.NewPCG(*seed, *seed))
	start := time.Now()

	for _, ws := range specs {
		wsStart := time.Now()
		if err := seedWorkspace(ctx, pool, rng, ws); err != nil {
			log.Fatalf("seed workspace %q: %v", ws.name, err)
		}
		fmt.Printf("workspace %-20s %8d opportunities in %s\n",
			ws.name, ws.count, time.Since(wsStart).Round(time.Millisecond))
	}

	if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
		log.Fatalf("analyze: %v", err)
	}
	fmt.Printf("done in %s\n", time.Since(start).Round(time.Millisecond))
}

func seedWorkspace(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, ws workspaceSpec) error {
	var workspaceID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO workspaces (name) VALUES (@name) RETURNING id`,
		pgx.NamedArgs{"name": ws.name},
	).Scan(&workspaceID); err != nil {
		return fmt.Errorf("insert workspace: %w", err)
	}

	stageIDs := make([]int64, len(stageSpecs))
	for i, s := range stageSpecs {
		id, err := stage.Create(ctx, pool, workspaceID, i, s.name)
		if err != nil {
			return err
		}
		stageIDs[i] = id
	}

	now := time.Now().UTC()
	windowStart := now.AddDate(0, -18, 0)

	i := 0
	next := func() ([]any, error) {
		if i >= ws.count {
			return nil, nil
		}
		i++

		stageIdx := weightedStageIndex(rng)
		stageID := stageIDs[stageIdx]
		stageName := stageSpecs[stageIdx].name

		createdAt := randomTimeBetween(rng, windowStart, now)
		updatedAt := randomTimeBetween(rng, createdAt, now)
		ownerID := rng.IntN(40) + 1
		valueCents := skewedValueCents(rng)
		status := statusFor(rng, stageName)
		name := fmt.Sprintf("%s deal #%d", ws.name, i)

		return []any{workspaceID, stageID, name, valueCents, status, ownerID, createdAt, updatedAt}, nil
	}

	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{"opportunities"},
		[]string{"workspace_id", "stage_id", "name", "value_cents", "status", "owner_id", "created_at", "updated_at"},
		pgx.CopyFromFunc(next),
	)
	if err != nil {
		return fmt.Errorf("copy opportunities: %w", err)
	}
	return nil
}

func statusFor(rng *rand.Rand, stageName string) string {
	switch stageName {
	case "Closed Won":
		return "won"
	case "Closed Lost":
		return "lost"
	default:
		switch r := rng.Float64(); {
		case r < 0.85:
			return "open"
		case r < 0.95:
			return "lost"
		default:
			return "abandoned"
		}
	}
}

// weightedStageIndex picks a stage index according to stageSpecs' weights.
func weightedStageIndex(rng *rand.Rand) int {
	total := 0.0
	for _, s := range stageSpecs {
		total += s.weight
	}
	r := rng.Float64() * total
	cum := 0.0
	for i, s := range stageSpecs {
		cum += s.weight
		if r < cum {
			return i
		}
	}
	return len(stageSpecs) - 1
}

func randomTimeBetween(rng *rand.Rand, from, to time.Time) time.Time {
	span := to.Sub(from)
	if span <= 0 {
		return from
	}
	return from.Add(time.Duration(rng.Int64N(int64(span))))
}

// skewedValueCents returns a value in [$500, $500,000] (cents), skewed
// toward the low end via exp() of a normal draw.
func skewedValueCents(rng *rand.Rand) int64 {
	const minCents = 500_00
	const maxCents = 500_000_00
	const baseCents = 5_000_00

	mult := math.Exp(rng.NormFloat64() * 0.9)
	v := int64(baseCents * mult)

	if v < minCents {
		return minCents
	}
	if v > maxCents {
		return maxCents
	}
	return v
}
