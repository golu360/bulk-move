package platform

import (
	"errors"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
)

// NewApp is the single place the Fiber app is configured. cmd/api and every
// handler test build the app through this function so they share the same
// middleware, limits and error handling.
func NewApp(log *slog.Logger) *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:      "opps-api",
		BodyLimit:    1 << 20, // 1 MB -> 413 body_too_large
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorHandler: ErrorHandler(log),
	})
	app.Use(recover.New())
	app.Use(RequestLogger(log))
	return app
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorHandler is the only place error responses are written. Handlers
// return errors; they never write an error body themselves.
func ErrorHandler(log *slog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		status, code, message := classifyError(err)
		if status == fiber.StatusInternalServerError && code == "internal" {
			log.Error("unhandled error", "error", err)
		}
		return c.Status(status).JSON(errorResponse{Error: errorDetail{Code: code, Message: message}})
	}
}

// classifyError maps an error to the status/code/message ErrorHandler will
// render. RequestLogger uses it too: Fiber only invokes ErrorHandler after
// the whole middleware chain (including RequestLogger's c.Next()) has
// returned, so reading c.Response().StatusCode() from within RequestLogger
// would still show the pre-error-handling status.
func classifyError(err error) (status int, code, message string) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status, apiErr.Code, apiErr.Message
	}

	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) {
		code := "internal"
		switch fiberErr.Code {
		case fiber.StatusNotFound:
			code = "route_not_found"
		case fiber.StatusMethodNotAllowed:
			code = "method_not_allowed"
		case fiber.StatusRequestEntityTooLarge:
			code = "body_too_large"
		}
		return fiberErr.Code, code, fiberErr.Message
	}

	return fiber.StatusInternalServerError, "internal", "internal server error"
}

// RequestLogger emits one slog line per request: method, path, status, latency.
func RequestLogger(log *slog.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()
		err := c.Next()

		status := c.Response().StatusCode()
		if err != nil {
			status, _, _ = classifyError(err)
		}

		log.Info("request",
			"method", c.Method(),
			"path", c.Path(),
			"status", status,
			"latency", time.Since(start).String(),
		)
		return err
	}
}
