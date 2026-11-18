DATABASE_URL ?= postgres://postgres:postgres@localhost:5434/opps?sslmode=disable

.PHONY: migrate-up migrate-down seed

migrate-up:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down 1

seed:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/seed -profile small -reset
