package platform

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HealthHandler serves GET /healthz. It takes no workspace header and only
// ever reports whether the database is reachable.
func HealthHandler(db *pgxpool.Pool) fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.Context(), 1*time.Second)
		defer cancel()

		if err := db.Ping(ctx); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"status": "unavailable"})
		}
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
	}
}
