package bulkmove

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MaxItems caps how many deals one bulk move may touch.
const MaxItems = 50_000

var (
	ErrKeyReused     = errors.New("idempotency key was already used with a different request")
	ErrTooMany       = fmt.Errorf("filter matches more than %d opportunities", MaxItems)
	ErrBadStage      = errors.New("target stage does not exist in this workspace")
	ErrInvalidFilter = errors.New("invalid filter")
	ErrNotFound      = errors.New("bulk move not found")
	// ErrLeaseLost: the job's lease token changed (another worker resumed
	// it) or it finished. The caller must stop without writing.
	ErrLeaseLost = errors.New("lease lost: job is owned by another worker")
)

// ClaimedJob is a job leased to one worker. Token fences every write.
type ClaimedJob struct {
	ID            int64
	WorkspaceID   int64
	TargetStageID int64
	Token         string
}

// Filter selects the deals to move. Every field is optional; an empty
// filter matches the whole workspace (still capped at MaxItems).
type Filter struct {
	StageID       *int64     `json:"stage_id,omitempty"`
	OwnerID       *int64     `json:"owner_id,omitempty"`
	Status        *string    `json:"status,omitempty"`
	MinValueCents *int64     `json:"min_value_cents,omitempty"`
	MaxValueCents *int64     `json:"max_value_cents,omitempty"`
	CreatedFrom   *time.Time `json:"created_from,omitempty"`
	CreatedTo     *time.Time `json:"created_to,omitempty"` // exclusive
}

// Request is the body of POST /bulk-moves.
type Request struct {
	Filter        Filter `json:"filter"`
	TargetStageID int64  `json:"target_stage_id"`
}

// Job is what a submit returns: the job's id, status and snapshot size.
type Job struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Total  int    `json:"total"`
}

// JobStatus is what GET /bulk-moves/{id} returns: progress and outcome.
type JobStatus struct {
	ID            int64      `json:"id"`
	Status        string     `json:"status"`
	TargetStageID int64      `json:"target_stage_id"`
	Total         int        `json:"total"`
	Moved         int        `json:"moved"`
	Skipped       int        `json:"skipped"`
	LastError     *string    `json:"last_error"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
}

var validStatuses = map[string]bool{"open": true, "won": true, "lost": true, "abandoned": true}

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidFilter}, a...)...)
}

// Validate checks the filter and normalizes times to UTC, so equal
// filters hash equally regardless of the offset the client sent.
func (f *Filter) Validate() error {
	if f.StageID != nil && *f.StageID <= 0 {
		return invalid("stage_id must be a positive integer")
	}
	if f.OwnerID != nil && *f.OwnerID <= 0 {
		return invalid("owner_id must be a positive integer")
	}
	if f.Status != nil && !validStatuses[*f.Status] {
		return invalid("status must be one of open, won, lost, abandoned")
	}
	if f.MinValueCents != nil && *f.MinValueCents < 0 {
		return invalid("min_value_cents must be >= 0")
	}
	if f.MaxValueCents != nil && *f.MaxValueCents < 0 {
		return invalid("max_value_cents must be >= 0")
	}
	if f.MinValueCents != nil && f.MaxValueCents != nil && *f.MinValueCents > *f.MaxValueCents {
		return invalid("min_value_cents must be <= max_value_cents")
	}
	if f.CreatedFrom != nil {
		t := f.CreatedFrom.UTC()
		f.CreatedFrom = &t
	}
	if f.CreatedTo != nil {
		t := f.CreatedTo.UTC()
		f.CreatedTo = &t
	}
	if f.CreatedFrom != nil && f.CreatedTo != nil && !f.CreatedFrom.Before(*f.CreatedTo) {
		return invalid("created_from must be before created_to")
	}
	return nil
}

// hash identifies the request for idempotency. It hashes the decoded,
// normalized struct, so whitespace, field order and time-zone offsets in the
// raw body don't matter. Call only after Validate.
func (r Request) hash() ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}
