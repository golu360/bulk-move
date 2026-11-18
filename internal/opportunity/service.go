package opportunity

import "context"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// Create validates the input and inserts the opportunity.
// Returns *ValidationError or ErrStageNotFound for client errors.
func (s *Service) Create(ctx context.Context, workspaceID int64, in CreateInput) (Opportunity, error) {
	if err := in.Normalize(); err != nil {
		return Opportunity{}, err
	}
	return s.repo.Create(ctx, workspaceID, in)
}

// ListStage returns one page of the stage. It fetches limit+1 rows so
// NextAfter is nil exactly on the last page (no trailing empty page).
// Returns ErrStageNotFound if the stage is not in the workspace.
func (s *Service) ListStage(ctx context.Context, workspaceID, stageID, after int64, limit int) (Page, error) {
	ok, err := s.repo.StageExists(ctx, workspaceID, stageID)
	if err != nil {
		return Page{}, err
	}
	if !ok {
		return Page{}, ErrStageNotFound
	}

	items, err := s.repo.ListByStage(ctx, workspaceID, stageID, after, limit+1)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		next := page.Items[limit-1].ID
		page.NextAfter = &next
	}
	return page, nil
}

// Move moves the opportunity to in.StageID. A move to its current stage is a
// no-op that returns the unchanged opportunity. Returns ErrNotFound,
// ErrStageNotFound or ErrBusy for client-visible failures.
func (s *Service) Move(ctx context.Context, workspaceID, id int64, in MoveInput) (Opportunity, error) {
	o, _, err := s.repo.MoveOpportunity(ctx, workspaceID, id, in.StageID)
	return o, err
}
