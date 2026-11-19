// Package stage holds the stage queries shared by several entities: the
// workspace-scoped existence check and stage creation. It has no HTTP surface.
package stage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx, so callers can run
// these queries inside their own transaction (e.g. MoveOpportunity,
// CreateJobWithSnapshot) or directly on the pool.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Exists reports whether stageID belongs to workspaceID. A stage in another
// workspace reports false, exactly like a stage that does not exist.
func Exists(ctx context.Context, q Querier, workspaceID, stageID int64) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM stages WHERE id = @stage_id AND workspace_id = @workspace_id)`,
		pgx.NamedArgs{"stage_id": stageID, "workspace_id": workspaceID}).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("check stage: %w", err)
	}
	return ok, nil
}

// Create inserts a stage at position in the workspace and returns its id.
// (workspace_id, position) is unique.
func Create(ctx context.Context, q Querier, workspaceID int64, position int, name string) (int64, error) {
	var id int64
	err := q.QueryRow(ctx,
		`INSERT INTO stages (workspace_id, position, name) VALUES (@workspace_id, @position, @name) RETURNING id`,
		pgx.NamedArgs{"workspace_id": workspaceID, "position": position, "name": name}).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert stage %q: %w", name, err)
	}
	return id, nil
}
