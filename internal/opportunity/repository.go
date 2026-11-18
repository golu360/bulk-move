package opportunity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// sqlstateLockNotAvailable is raised when lock_timeout expires.
const sqlstateLockNotAvailable = "55P03"

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

const returningCols = `id, name, value_cents, status, stage_id, owner_id, version, created_at, updated_at`

func scanOpportunity(row pgx.Row) (Opportunity, error) {
	var o Opportunity
	err := row.Scan(&o.ID, &o.Name, &o.ValueCents, &o.Status, &o.StageID, &o.OwnerID,
		&o.Version, &o.CreatedAt, &o.UpdatedAt)
	// API timestamps are RFC 3339 in UTC regardless of the session time zone.
	o.CreatedAt, o.UpdatedAt = o.CreatedAt.UTC(), o.UpdatedAt.UTC()
	return o, err
}

// Create inserts the opportunity only if stage_id belongs to the workspace.
// The stage check and the insert are one statement, so there is no window
// between them. No row returned means the stage is not in this workspace.
func (r *Repository) Create(ctx context.Context, workspaceID int64, in CreateInput) (Opportunity, error) {
	row := r.db.QueryRow(ctx, `
		INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, status, owner_id)
		SELECT $1, $2, $3, $4, $5, $6
		WHERE EXISTS (SELECT 1 FROM stages WHERE id = $2 AND workspace_id = $1)
		RETURNING `+returningCols,
		workspaceID, in.StageID, in.Name, in.ValueCents, in.Status, in.OwnerID)

	o, err := scanOpportunity(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Opportunity{}, ErrStageNotFound
	}
	if err != nil {
		return Opportunity{}, fmt.Errorf("insert opportunity: %w", err)
	}
	return o, nil
}

// StageExists reports whether the stage belongs to the workspace.
func (r *Repository) StageExists(ctx context.Context, workspaceID, stageID int64) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM stages WHERE id = $1 AND workspace_id = $2)`,
		stageID, workspaceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check stage: %w", err)
	}
	return exists, nil
}

// ListByStage returns up to limit opportunities in the stage with id > after,
// ordered by id. Keyset pagination served by opportunities_ws_stage_id.
func (r *Repository) ListByStage(ctx context.Context, workspaceID, stageID, after int64, limit int) ([]Opportunity, error) {
	rows, err := r.db.Query(ctx, `
		SELECT `+returningCols+`
		FROM opportunities
		WHERE workspace_id = $1 AND stage_id = $2 AND id > $3
		ORDER BY id
		LIMIT $4`,
		workspaceID, stageID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list opportunities: %w", err)
	}
	defer rows.Close()

	items := make([]Opportunity, 0, limit)
	for rows.Next() {
		o, err := scanOpportunity(rows)
		if err != nil {
			return nil, fmt.Errorf("scan opportunity: %w", err)
		}
		items = append(items, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list opportunities: %w", err)
	}
	return items, nil
}

// MoveOpportunity moves one opportunity to targetStageID and records a
// 'manual' transition, all in one transaction. moved is false (and nothing is
// written) when the opportunity is already in the target stage.
//
// INVARIANT: every write to opportunities must bump version. The bulk move
// job relies on it to detect deals edited after its snapshot was taken.
func (r *Repository) MoveOpportunity(ctx context.Context, workspaceID, id, targetStageID int64) (o Opportunity, moved bool, err error) {
	err = pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		// Never queue for long behind another writer (user or bulk chunk).
		if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', '2s', true)`); err != nil {
			return fmt.Errorf("set lock_timeout: %w", err)
		}

		var fromStage int64
		err := tx.QueryRow(ctx,
			`SELECT stage_id FROM opportunities WHERE id = $1 AND workspace_id = $2 FOR UPDATE`,
			id, workspaceID).Scan(&fromStage)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock opportunity: %w", err)
		}

		var stageOK bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM stages WHERE id = $1 AND workspace_id = $2)`,
			targetStageID, workspaceID).Scan(&stageOK); err != nil {
			return fmt.Errorf("check stage: %w", err)
		}
		if !stageOK {
			return ErrStageNotFound
		}

		// stage_id <> $3 makes "already in the target stage" a zero-row update:
		// nothing is written, version is not bumped, no transition is recorded.
		o, err = scanOpportunity(tx.QueryRow(ctx, `
			UPDATE opportunities
			SET stage_id = $3, version = version + 1, updated_at = now()
			WHERE id = $1 AND workspace_id = $2 AND stage_id <> $3
			RETURNING `+returningCols,
			id, workspaceID, targetStageID))
		if errors.Is(err, pgx.ErrNoRows) {
			// The row exists (we hold its lock), so no row means no-op.
			o, err = scanOpportunity(tx.QueryRow(ctx,
				`SELECT `+returningCols+` FROM opportunities WHERE id = $1 AND workspace_id = $2`,
				id, workspaceID))
			if err != nil {
				return fmt.Errorf("read opportunity: %w", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("update opportunity: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO stage_transitions
			       (workspace_id, opportunity_id, from_stage_id, to_stage_id, source)
			VALUES ($1, $2, $3, $4, 'manual')`,
			workspaceID, id, fromStage, targetStageID); err != nil {
			return fmt.Errorf("insert transition: %w", err)
		}
		moved = true
		return nil
	})

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlstateLockNotAvailable {
		return Opportunity{}, false, ErrBusy
	}
	if err != nil {
		return Opportunity{}, false, err
	}
	return o, moved, nil
}
