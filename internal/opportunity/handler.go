package opportunity

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"

	"opps/internal/platform"
)

const requestTimeout = 5 * time.Second

type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// Register mounts the opportunity routes on a router that already has
// platform.RequireWorkspace applied.
func (h *Handler) Register(r fiber.Router) {
	r.Post("/opportunities", h.Create)
	r.Post("/opportunities/:id/move", h.Move)
	r.Get("/stages/:stageId/opportunities", h.ListStage)
}

// Move handles POST /opportunities/{id}/move.
func (h *Handler) Move(c fiber.Ctx) error {
	id, err := platform.PathID(c, "id")
	if err != nil {
		return err
	}
	var in MoveInput
	if err := platform.DecodeStrict(c, &in); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	o, err := h.svc.Move(ctx, platform.WorkspaceID(c), id, in)
	switch {
	case err == nil:
		return c.JSON(o)
	case errors.Is(err, ErrNotFound):
		return platform.NotFound("opportunity_not_found",
			fmt.Sprintf("opportunity %d does not exist in this workspace", id))
	case errors.Is(err, ErrStageNotFound):
		return platform.Unprocessable("stage_not_found",
			fmt.Sprintf("stage %d does not exist in this workspace", in.StageID))
	case errors.Is(err, ErrBusy):
		c.Set("Retry-After", "1")
		return platform.Unavailable("busy", "opportunity is being modified by another request; retry shortly")
	default:
		return err
	}
}

// ListStage handles GET /stages/{stageId}/opportunities?limit=&after=.
func (h *Handler) ListStage(c fiber.Ctx) error {
	stageID, err := platform.PathID(c, "stageId")
	if err != nil {
		return err
	}

	limit := DefaultPageLimit
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > MaxPageLimit {
			return platform.BadRequest("invalid_query",
				fmt.Sprintf("limit must be an integer between 1 and %d", MaxPageLimit))
		}
		limit = n
	}

	var after int64
	if raw := c.Query("after"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			return platform.BadRequest("invalid_query", "after must be a non-negative integer opportunity id")
		}
		after = n
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	page, err := h.svc.ListStage(ctx, platform.WorkspaceID(c), stageID, after, limit)
	if errors.Is(err, ErrStageNotFound) {
		return platform.NotFound("stage_not_found",
			fmt.Sprintf("stage %d does not exist in this workspace", stageID))
	}
	if err != nil {
		return err
	}
	return c.JSON(page)
}

// Create handles POST /opportunities.
func (h *Handler) Create(c fiber.Ctx) error {
	var in CreateInput
	if err := platform.DecodeStrict(c, &in); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(c.Context(), requestTimeout)
	defer cancel()

	o, err := h.svc.Create(ctx, platform.WorkspaceID(c), in)
	if err != nil {
		return toAPIError(err, in.StageID)
	}

	c.Location(fmt.Sprintf("/opportunities/%d", o.ID))
	return c.Status(fiber.StatusCreated).JSON(o)
}

func toAPIError(err error, stageID int64) error {
	var ve *ValidationError
	switch {
	case errors.As(err, &ve):
		return platform.Unprocessable("validation_failed", ve.Reason)
	case errors.Is(err, ErrStageNotFound):
		return platform.Unprocessable("stage_not_found",
			fmt.Sprintf("stage %d does not exist in this workspace", stageID))
	default:
		return err // ErrorHandler logs it and returns 500 internal
	}
}
