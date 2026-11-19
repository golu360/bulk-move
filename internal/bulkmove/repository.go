package bulkmove

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/stage"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

// filterCols maps each filter field to its fixed SQL condition. Column
// names, operators and parameter names come only from this table; values are
// always bound as named parameters, never interpolated.
var filterCols = []struct {
	param, cond string
	value       func(Filter) any // nil when the field is unset
}{
	{"filter_stage_id", "stage_id =", func(f Filter) any { return deref(f.StageID) }},
	{"filter_owner_id", "owner_id =", func(f Filter) any { return deref(f.OwnerID) }},
	{"filter_status", "status =", func(f Filter) any { return deref(f.Status) }},
	{"filter_min_value_cents", "value_cents >=", func(f Filter) any { return deref(f.MinValueCents) }},
	{"filter_max_value_cents", "value_cents <=", func(f Filter) any { return deref(f.MaxValueCents) }},
	{"filter_created_from", "created_at >=", func(f Filter) any { return deref(f.CreatedFrom) }},
	{"filter_created_to", "created_at <", func(f Filter) any { return deref(f.CreatedTo) }},
}

// deref returns *p, or an untyped nil if p is nil.
func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// conditions returns " AND col op @filter_x" for every set field of f and
// stores each value in args under its fixed parameter name. It lives here,
// not in model.go, because SQL belongs only in repository.go.
func (f Filter) conditions(args pgx.NamedArgs) string {
	var sb strings.Builder
	for _, c := range filterCols {
		v := c.value(f)
		if v == nil {
			continue
		}
		args[c.param] = v
		sb.WriteString(" AND " + c.cond + " @" + c.param)
	}
	return sb.String()
}

// CreateJobWithSnapshot creates the job and its item snapshot in ONE
// transaction, or returns the existing job for a replayed idempotency key.
//
// The unique index (workspace_id, idempotency_key) arbitrates concurrent
// submits: a second INSERT with the same key blocks until the first
// transaction ends, then does nothing, and the existing row is read back.
// created is false for a replay.
func (r *Repository) CreateJobWithSnapshot(ctx context.Context, workspaceID int64, key string, requestHash []byte, req Request) (job Job, created bool, err error) {
	filterJSON, err := json.Marshal(req.Filter)
	if err != nil {
		return Job{}, false, fmt.Errorf("marshal filter: %w", err)
	}

	args := pgx.NamedArgs{
		"workspace_id":    workspaceID,
		"idempotency_key": key,
		"request_hash":    requestHash,
		"filter":          filterJSON,
		"target_stage_id": req.TargetStageID,
		"limit":           MaxItems + 1,
	}

	err = pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		// 1. Target stage must belong to the workspace.
		stageOK, err := stage.Exists(ctx, tx, workspaceID, req.TargetStageID)
		if err != nil {
			return err
		}
		if !stageOK {
			return ErrBadStage
		}

		// 2. Insert the job; the unique index decides who owns the key.
		var jobID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO bulk_jobs (workspace_id, idempotency_key, request_hash, filter, target_stage_id)
			VALUES (@workspace_id, @idempotency_key, @request_hash, @filter, @target_stage_id)
			ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
			RETURNING id`,
			args).Scan(&jobID)

		// 3. Key seen before: same request → replay, different → reused.
		if errors.Is(err, pgx.ErrNoRows) {
			var storedHash []byte
			if err := tx.QueryRow(ctx, `
				SELECT id, request_hash, total FROM bulk_jobs
				WHERE workspace_id = @workspace_id AND idempotency_key = @idempotency_key`,
				args).Scan(&job.ID, &storedHash, &job.Total); err != nil {
				return fmt.Errorf("load existing job: %w", err)
			}
			if !bytes.Equal(storedHash, requestHash) {
				return ErrKeyReused
			}
			// A replay returns the same answer the original submit did.
			job.Status = "pending"
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert job: %w", err)
		}

		// 4. Snapshot the matching deals. seq runs 1..total in id order.
		args["job_id"] = jobID
		conds := req.Filter.conditions(args)
		tag, err := tx.Exec(ctx, `
			INSERT INTO bulk_job_items (job_id, seq, opportunity_id, snapshot_version)
			SELECT @job_id, row_number() OVER (ORDER BY id), id, version
			FROM (SELECT id, version FROM opportunities
			      WHERE workspace_id = @workspace_id`+conds+`
			      ORDER BY id LIMIT @limit) matched`, args)
		if err != nil {
			return fmt.Errorf("snapshot items: %w", err)
		}
		total := tag.RowsAffected()
		if total > MaxItems {
			return ErrTooMany // rolls back the job and its items
		}

		// 5. Record the snapshot size.
		if _, err := tx.Exec(ctx, `UPDATE bulk_jobs SET total = @total WHERE id = @job_id`,
			pgx.NamedArgs{"job_id": jobID, "total": total}); err != nil {
			return fmt.Errorf("set total: %w", err)
		}
		job = Job{ID: jobID, Status: "pending", Total: int(total)}
		created = true
		return nil
	})
	if err != nil {
		return Job{}, false, err
	}
	return job, created, nil
}
