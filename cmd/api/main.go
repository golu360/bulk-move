package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

	"opps/internal/opportunity"
	"opps/internal/platform"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := platform.NewPool(ctx, dbURL, 20, "api")
	if err != nil {
		log.Error("connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	app := platform.NewApp(log)
	app.Get("/healthz", platform.HealthHandler(pool))

	// Everything except /healthz requires X-Workspace-Id.
	ws := app.Group("", platform.RequireWorkspace)
	opportunity.NewHandler(opportunity.NewService(opportunity.NewRepository(pool))).Register(ws)

	go func() {
		if err := app.Listen(":8080", fiber.ListenConfig{DisableStartupMessage: true}); err != nil {
			log.Error("listen", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("shutdown", "error", err)
	}
}
