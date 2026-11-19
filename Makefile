DATABASE_URL ?= postgres://postgres:postgres@localhost:5434/opps?sslmode=disable

.PHONY: migrate-up migrate-down seed-large seed-small bench-large bench-small api worker test

migrate-up:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down 1

PSQL = docker compose exec -T db psql -U postgres -d opps -v ON_ERROR_STOP=1

seed-large:
	$(PSQL) < seeds/large.sql

seed-small:
	$(PSQL) < seeds/small.sql

# Each bench re-seeds first, so runs are comparable.
bench-large: seed-large
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/bench

bench-small: seed-small
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/bench

api:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/api

worker:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/worker

# Everything in Docker: db, migrate, small seed, api, worker, go test.
test:
	docker compose run --build --rm tests
