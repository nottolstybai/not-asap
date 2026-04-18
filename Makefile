.PHONY: help build vet tidy test test-unit test-integration testdb testdb-down migrate-up migrate-down coverage clean

DSN ?= postgres://notasap:notasap@localhost:5432/notasap_test?sslmode=disable
MIGRATIONS_DIR := database/migrations
GOOSE := go run github.com/pressly/goose/v3/cmd/goose@latest
# -race requires CGO. Windows users without a C toolchain can override with `make test RACE=`.
RACE ?= -race

help: ## Show this help
	@echo "Available targets:"
	@echo "  build               - go build ./..."
	@echo "  vet                 - go vet ./..."
	@echo "  tidy                - go mod tidy"
	@echo "  test                - run unit + integration tests (needs testdb up)"
	@echo "  test-unit           - run only unit tests (no DB)"
	@echo "  test-integration    - run only integration tests (needs testdb up)"
	@echo "  testdb              - recreate postgres container + apply migrations"
	@echo "  testdb-down         - stop postgres container"
	@echo "  migrate-up          - apply all migrations to DSN"
	@echo "  migrate-down        - rollback one migration on DSN"
	@echo "  coverage            - go test -coverprofile"

build:
	go build ./...

vet:
	go vet ./...

tidy:
	go mod tidy

test-unit:
	go test -count=1 $(RACE) ./...

test-integration:
	go test -count=1 $(RACE) -tags=integration ./...

test: test-unit test-integration

testdb: testdb-down
	docker compose up -d postgres
	@echo "Waiting for postgres to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' notasap-testdb 2>/dev/null)" = "healthy" ]; do \
		sleep 1; \
	done
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DSN)" up

testdb-down:
	-docker compose down -v

migrate-up:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DSN)" up

migrate-down:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DSN)" down

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

clean:
	rm -f coverage.out coverage.html
