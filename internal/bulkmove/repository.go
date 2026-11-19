package bulkmove

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"opps/internal/stage"
)

type Repository struct {
	db *pgxpool.Pool

	// beforeCommit is a test hook: if set, ApplyChunk calls it inside the
	// chunk transaction after all writes and before COMMIT. Only tests in
	// this package set it (to simulate a crash or a failing chunk).
	beforeCommit func() error
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
				SELECT id, request_hash, status, total FROM bulk_jobs
				WHERE workspace_id = @workspace_id AND idempotency_key = @idempotency_key`,
				args).Scan(&job.ID, &storedHash, &job.Status, &job.Total); err != nil {
				return fmt.Errorf("load existing job: %w", err)
			}
			if !bytes.Equal(storedHash, requestHash) {
				return ErrKeyReused
			}
			// A replay is the same job with its CURRENT status. It never
			// restarts the job: a failed or completed job stays as it is.
			// To run the same move again, submit with a new key.
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

// GetJob returns a job of the workspace. A job of another workspace is
// indistinguishable from a missing one: both are ErrNotFound.
func (r *Repository) GetJob(ctx context.Context, workspaceID, jobID int64) (JobStatus, error) {
	var j JobStatus
	err := r.db.QueryRow(ctx, `
		SELECT id, status, target_stage_id, total, moved, skipped, last_error,
		       created_at, started_at, finished_at
		FROM bulk_jobs
		WHERE id = @job_id AND workspace_id = @workspace_id`,
		pgx.NamedArgs{"job_id": jobID, "workspace_id": workspaceID},
	).Scan(&j.ID, &j.Status, &j.TargetStageID, &j.Total, &j.Moved, &j.Skipped, &j.LastError,
		&j.CreatedAt, &j.StartedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobStatus{}, ErrNotFound
	}
	if err != nil {
		return JobStatus{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// ---------------------------------------------------------------------------
// Worker queries: claim, apply a chunk, mark failed.
// ---------------------------------------------------------------------------

const sqlstateUniqueViolation = "23505"

// newLeaseToken returns 16 random bytes, hex-encoded. A new token is minted
// for every claim; only the current token holder may write to the job.
func newLeaseToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("lease token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// Claim takes the oldest claimable job and leases it to token for leaseTTL.
// Claimable means: pending in a workspace with no running job, or running
// with an expired lease (its worker died; resume it). Returns nil, nil when
// nothing is claimable.
//
// Isolation (one running job per workspace) is enforced twice: NOT EXISTS
// filters candidates, and the partial unique index bulk_jobs_one_running
// rejects the loser when two workers race to start two jobs in the same
// workspace. That rejection (23505) is expected and reported as nil, nil.
func (r *Repository) Claim(ctx context.Context, token string, leaseTTL time.Duration) (*ClaimedJob, error) {
	job := ClaimedJob{Token: token}
	err := r.db.QueryRow(ctx, `
		UPDATE bulk_jobs SET
			status           = 'running',
			lease_token      = @token,
			lease_expires_at = now() + make_interval(secs => @lease_secs),
			claims           = claims + 1,
			started_at       = coalesce(started_at, now())
		WHERE id = (
			SELECT id FROM bulk_jobs c
			WHERE (c.status = 'pending' AND NOT EXISTS (
			          SELECT 1 FROM bulk_jobs r
			          WHERE r.workspace_id = c.workspace_id AND r.status = 'running'))
			   OR (c.status = 'running' AND c.lease_expires_at < now())
			ORDER BY c.created_at, c.id
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING id, workspace_id, target_stage_id`,
		pgx.NamedArgs{"token": token, "lease_secs": leaseTTL.Seconds()},
	).Scan(&job.ID, &job.WorkspaceID, &job.TargetStageID)

	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case errors.As(err, &pgErr) && pgErr.Code == sqlstateUniqueViolation:
		return nil, nil // lost the race to start a job in this workspace
	case err != nil:
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return &job, nil
}

// ApplyChunk processes the next cfg.ChunkSize snapshot items of job.
//
// INVARIANT: everything happens in ONE transaction. The deal updates, their
// transition rows, the cursor and counters, and the lease renewal commit
// together or not at all. Never split this across calls, goroutines or
// layers: it is what makes a crash at any point safe to resume.
//
// The transaction is explicitly READ COMMITTED: when FOR UPDATE waits on a
// row a user is editing, Postgres re-checks the WHERE clause against the
// row's new version after the user commits (version changed → deal
// excluded). Under REPEATABLE READ that wait would instead abort the whole
// chunk with a serialization failure.
//
// Returns done=true once the job is completed, ErrLeaseLost if another
// worker now owns the job (nothing was written).
func (r *Repository) ApplyChunk(ctx context.Context, job *ClaimedJob, cfg Config) (done bool, err error) {
	args := pgx.NamedArgs{
		"job_id":          job.ID,
		"token":           job.Token,
		"workspace_id":    job.WorkspaceID,
		"target_stage_id": job.TargetStageID,
		"lease_secs":      cfg.LeaseTTL.Seconds(),
		"lock_timeout":    fmt.Sprintf("%dms", cfg.LockTimeout.Milliseconds()),
	}

	var finished bool
	err = pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		// 1. Never wait long behind a user's edit; the chunk is retried.
		if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', @lock_timeout, true)`, args); err != nil {
			return fmt.Errorf("set lock_timeout: %w", err)
		}

		// 2. Fence + read cursor. Only the current token holder gets a row.
		// Holding this row lock also stops anyone claiming the job mid-chunk
		// (Claim uses SKIP LOCKED).
		var cursor, total int
		err := tx.QueryRow(ctx, `
			SELECT cursor_seq, total FROM bulk_jobs
			WHERE id = @job_id AND lease_token = @token
			FOR UPDATE`, args).Scan(&cursor, &total)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return fmt.Errorf("fence job: %w", err)
		}

		// 3. Nothing left (e.g. a job with total 0): complete it.
		if cursor >= total {
			if _, err := tx.Exec(ctx, `
				UPDATE bulk_jobs
				SET status = 'completed', finished_at = now(), lease_token = NULL
				WHERE id = @job_id`, args); err != nil {
				return fmt.Errorf("complete job: %w", err)
			}
			finished = true
			return nil
		}

		// 4. Move the chunk in one statement. A deal moves only if it is
		// unchanged since the snapshot (version) and not already in the
		// target stage; everything else in the range counts as skipped.
		end := min(cursor+cfg.ChunkSize, total)
		args["cursor"], args["end"] = cursor, end
		tag, err := tx.Exec(ctx, `
			WITH chunk AS (
				SELECT o.id, o.stage_id AS from_stage
				FROM bulk_job_items i
				JOIN opportunities o ON o.id = i.opportunity_id
				WHERE i.job_id = @job_id AND i.seq > @cursor AND i.seq <= @end
				  AND o.workspace_id = @workspace_id
				  AND o.version = i.snapshot_version
				  AND o.stage_id <> @target_stage_id
				ORDER BY o.id
				FOR UPDATE OF o
			), moved AS (
				UPDATE opportunities o
				SET stage_id = @target_stage_id, version = o.version + 1, updated_at = now()
				FROM chunk WHERE o.id = chunk.id
				RETURNING o.id
			)
			INSERT INTO stage_transitions
			       (workspace_id, opportunity_id, from_stage_id, to_stage_id, source, bulk_job_id)
			SELECT @workspace_id, chunk.id, chunk.from_stage, @target_stage_id, 'bulk', @job_id
			FROM chunk JOIN moved ON moved.id = chunk.id`, args)
		if err != nil {
			return fmt.Errorf("apply chunk: %w", err)
		}
		moved := int(tag.RowsAffected())

		// 5. Advance the cursor, count, renew the lease, and complete the job
		// if this was the last chunk — all in the same transaction.
		finished = end == total
		args["chunk_moved"], args["chunk_skipped"], args["finished"] = moved, (end-cursor)-moved, finished
		if _, err := tx.Exec(ctx, `
			UPDATE bulk_jobs SET
				cursor_seq       = @end,
				moved            = moved + @chunk_moved,
				skipped          = skipped + @chunk_skipped,
				last_progress_at = now(),
				lease_expires_at = now() + make_interval(secs => @lease_secs),
				status           = CASE WHEN @finished THEN 'completed' ELSE status END,
				finished_at      = CASE WHEN @finished THEN now() ELSE finished_at END,
				lease_token      = CASE WHEN @finished THEN NULL ELSE lease_token END
			WHERE id = @job_id`, args); err != nil {
			return fmt.Errorf("advance cursor: %w", err)
		}

		// 6. Test hook: after all writes, before COMMIT.
		if r.beforeCommit != nil {
			if err := r.beforeCommit(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return finished, nil
}

// MarkFailed marks the job failed, fenced by the lease token so a replaced
// worker cannot fail a job someone else is now running. Returns whether the
// job was updated.
func (r *Repository) MarkFailed(ctx context.Context, job *ClaimedJob, reason string) (bool, error) {
	tag, err := r.db.Exec(ctx, `
		UPDATE bulk_jobs
		SET status = 'failed', last_error = @reason, finished_at = now(), lease_token = NULL
		WHERE id = @job_id AND lease_token = @token`,
		pgx.NamedArgs{"job_id": job.ID, "token": job.Token, "reason": reason})
	if err != nil {
		return false, fmt.Errorf("mark failed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
