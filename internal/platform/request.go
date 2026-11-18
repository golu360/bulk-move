package platform

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/gofiber/fiber/v3"
)

type workspaceKey struct{}

// RequireWorkspace parses X-Workspace-Id into a positive int64 and stores it
// in c.Locals. Every route except /healthz sits behind this middleware.
func RequireWorkspace(c fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Get("X-Workspace-Id"), 10, 64)
	if err != nil || id <= 0 {
		return BadRequest("invalid_workspace_header", "X-Workspace-Id header must be a positive integer")
	}
	c.Locals(workspaceKey{}, id)
	return c.Next()
}

// WorkspaceID reads the id RequireWorkspace stored for this request.
func WorkspaceID(c fiber.Ctx) int64 {
	id, _ := c.Locals(workspaceKey{}).(int64)
	return id
}

// DecodeStrict decodes the request body into v, rejecting unknown fields.
// Use this instead of c.Bind(), which accepts unknown fields by default.
func DecodeStrict(c fiber.Ctx, v any) error {
	dec := json.NewDecoder(bytes.NewReader(c.Body()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return BadRequest("invalid_json", "request body is not valid JSON: "+err.Error())
	}
	return nil
}

// PathID parses a path parameter as a positive int64. Fiber's generic param
// helpers fall back to zero on bad input, so they can't distinguish
// "invalid" from "0" — this can.
func PathID(c fiber.Ctx, name string) (int64, error) {
	id, err := strconv.ParseInt(c.Params(name), 10, 64)
	if err != nil || id <= 0 {
		return 0, BadRequest("invalid_path_param", "path parameter "+name+" must be a positive integer")
	}
	return id, nil
}
