package bulkmove

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"

	"opps/internal/platform"
)

const (
	requestTimeout = 10 * time.Second // the snapshot of up to 50k rows runs inline
	maxKeyLen      = 200

	// ReplayedHeader is set to "true" on a response for a replayed key.
	ReplayedHeader = "X-Idempotency-Replayed"
)

type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// Register mounts the bulk move routes on a router that already has
// platform.RequireWorkspace applied.
func (h *Handler) Register(r fiber.Router) {
	r.Post("/bulk-moves", h.Submit)
	r.Get("/bulk-moves/:id", h.Get)
}

// Get handles GET /bulk-moves/{id}. Another workspace's job is a 404.
func (h *Handler) Get(c fiber.Ctx) error {
	id, err := platform.PathID(c, "id")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	job, err := h.svc.Get(ctx, platform.WorkspaceID(c), id)
	switch {
	case err == nil:
		return c.JSON(job)
	case errors.Is(err, ErrNotFound):
		return platform.NotFound("bulk_move_not_found",
			fmt.Sprintf("bulk move %d does not exist in this workspace", id))
	default:
		return err
	}
}

// validKey: 1–200 printable ASCII characters.
func validKey(k string) bool {
	if len(k) == 0 || len(k) > maxKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

// Submit handles POST /bulk-moves.
func (h *Handler) Submit(c fiber.Ctx) error {
	// Fiber reuses request buffers; copy the key before handing it on.
	key := strings.Clone(c.Get("Idempotency-Key"))
	if !validKey(key) {
		return platform.BadRequest("missing_idempotency_key",
			fmt.Sprintf("Idempotency-Key header is required: 1-%d printable ASCII characters", maxKeyLen))
	}

	var req Request
	if err := platform.DecodeStrict(c, &req); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	job, created, err := h.svc.Submit(ctx, platform.WorkspaceID(c), key, req)
	switch {
	case err == nil:
	case errors.Is(err, ErrInvalidFilter):
		return platform.Unprocessable("invalid_filter", err.Error())
	case errors.Is(err, ErrBadStage):
		return platform.Unprocessable("target_stage_not_found",
			fmt.Sprintf("stage %d does not exist in this workspace", req.TargetStageID))
	case errors.Is(err, ErrKeyReused):
		return platform.Unprocessable("idempotency_key_reused",
			"this Idempotency-Key was already used with a different request")
	case errors.Is(err, ErrTooMany):
		return platform.Unprocessable("too_many_matches",
			fmt.Sprintf("filter matches more than %d opportunities; narrow it and resubmit", MaxItems))
	default:
		return err
	}

	if !created {
		c.Set(ReplayedHeader, "true")
	}
	c.Location(fmt.Sprintf("/bulk-moves/%d", job.ID))
	return c.Status(fiber.StatusAccepted).JSON(job)
}
