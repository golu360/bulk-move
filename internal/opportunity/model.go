package opportunity

import (
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

// Opportunity is a deal in a workspace's pipeline.
type Opportunity struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	ValueCents int64     `json:"value_cents"`
	Status     string    `json:"status"`
	StageID    int64     `json:"stage_id"`
	OwnerID    int64     `json:"owner_id"`
	Version    int       `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

const maxNameLen = 200

var validStatuses = map[string]bool{"open": true, "won": true, "lost": true, "abandoned": true}

// CreateInput is the body of POST /opportunities.
type CreateInput struct {
	Name       string `json:"name"`
	ValueCents int64  `json:"value_cents"`
	StageID    int64  `json:"stage_id"`
	OwnerID    int64  `json:"owner_id"`
	Status     string `json:"status"` // optional, defaults to "open"
}

var (
	// ErrStageNotFound: the stage does not exist in this workspace.
	ErrStageNotFound = errors.New("stage not found")
	// ErrNotFound: the opportunity does not exist in this workspace.
	ErrNotFound = errors.New("opportunity not found")
	// ErrBusy: the row lock was not obtained within lock_timeout.
	ErrBusy = errors.New("opportunity is locked by another transaction")
)

// MoveInput is the body of POST /opportunities/{id}/move.
type MoveInput struct {
	StageID int64 `json:"stage_id"`
}

// ValidationError carries a human-readable reason for a 422 validation_failed.
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return e.Reason }

// Normalize applies defaults and validates the input.
func (in *CreateInput) Normalize() error {
	if in.Status == "" {
		in.Status = "open"
	}
	switch {
	case in.Name == "":
		return &ValidationError{"name is required"}
	case utf8.RuneCountInString(in.Name) > maxNameLen:
		return &ValidationError{fmt.Sprintf("name must be at most %d characters", maxNameLen)}
	case in.ValueCents < 0:
		return &ValidationError{"value_cents must be >= 0"}
	case in.OwnerID <= 0:
		return &ValidationError{"owner_id must be a positive integer"}
	case !validStatuses[in.Status]:
		return &ValidationError{"status must be one of open, won, lost, abandoned"}
	}
	return nil
}

const (
	DefaultPageLimit = 50
	MaxPageLimit     = 200
)

// Page is one keyset page of a stage listing. NextAfter is nil on the last page.
type Page struct {
	Items     []Opportunity `json:"items"`
	NextAfter *int64        `json:"next_after"`
}
