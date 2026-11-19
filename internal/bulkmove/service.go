package bulkmove

import "context"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// Submit validates the request and creates the job with its snapshot, or
// returns the original job when the idempotency key is replayed with the same
// request (created=false). Returns ErrInvalidFilter, ErrBadStage,
// ErrKeyReused or ErrTooMany for client errors.
func (s *Service) Submit(ctx context.Context, workspaceID int64, key string, req Request) (Job, bool, error) {
	if err := req.Filter.Validate(); err != nil {
		return Job{}, false, err
	}
	h, err := req.hash()
	if err != nil {
		return Job{}, false, err
	}
	return s.repo.CreateJobWithSnapshot(ctx, workspaceID, key, h, req)
}
