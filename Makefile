# go-log-aggregator
#
# `make help` lists targets. `make dev` is the entry point for a fresh clone.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

COMPOSE_FILE       := deploy/docker-compose.yml
COMPOSE_SCALE_FILE := deploy/docker-compose.scale.yml
COMPOSE            := docker compose -f $(COMPOSE_FILE)
COMPOSE_SCALE      := docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_SCALE_FILE)

VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

VERSION_PKG := github.com/jamespolk/go-log-aggregator/internal/version
LDFLAGS     := -s -w \
	-X $(VERSION_PKG).Version=$(VERSION) \
	-X $(VERSION_PKG).Commit=$(COMMIT) \
	-X $(VERSION_PKG).BuildDate=$(BUILD_DATE)

BIN_DIR := bin
CMDS    := $(notdir $(wildcard cmd/*))

export VERSION COMMIT BUILD_DATE

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---- build ----------------------------------------------------------------

.PHONY: build
build: ## Build every binary in cmd/ into bin/
	@mkdir -p $(BIN_DIR)
	@for cmd in $(CMDS); do \
		echo "building $$cmd"; \
		go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$$cmd ./cmd/$$cmd; \
	done

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) coverage.out coverage.html

## ---- quality --------------------------------------------------------------

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -l -w .

.PHONY: tidy
tidy: ## Sync go.mod and go.sum
	go mod tidy

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run unit tests
	go test ./...

.PHONY: test-race
test-race: ## Run unit tests under the race detector
	go test -race ./...

.PHONY: cover
cover: ## Run tests with coverage and open the HTML report
	go test -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "coverage report: coverage.html"

.PHONY: bench
bench: ## Run Go benchmarks
	go test -run '^$$' -bench . -benchmem ./...

.PHONY: vulncheck
vulncheck: ## Scan dependencies for known vulnerabilities
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: ci
ci: tidy-check fmt-check vet lint test-race ## Everything CI enforces

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-clean
	@out=$$(gofmt -l .); \
	if [[ -n "$$out" ]]; then echo "not gofmt-clean:"; echo "$$out"; exit 1; fi

.PHONY: tidy-check
tidy-check: ## Fail if go.mod or go.sum would change
	@cp go.mod go.mod.bak && cp go.sum go.sum.bak
	@go mod tidy
	@if ! diff -q go.mod go.mod.bak >/dev/null || ! diff -q go.sum go.sum.bak >/dev/null; then \
		mv go.mod.bak go.mod; mv go.sum.bak go.sum; \
		echo "go.mod/go.sum are stale; run 'make tidy'"; exit 1; \
	fi
	@rm -f go.mod.bak go.sum.bak

## ---- docker ---------------------------------------------------------------

.PHONY: docker-build
docker-build: ## Build the collector image
	$(COMPOSE) build

.PHONY: dev
dev: ## Start the full dev stack in the background (one collector, fixed ports)
	$(COMPOSE) up -d --build --wait --wait-timeout 240
	@echo
	@echo "  api      http://127.0.0.1:8080/healthz"
	@echo "  metrics  http://127.0.0.1:9090/metrics"
	@echo "  nats     http://127.0.0.1:8222/healthz"
	@echo "  postgres postgres://logagg:logagg@127.0.0.1:5432/logagg"

.PHONY: dev-down
dev-down: ## Stop the dev stack, keeping volumes
	$(COMPOSE) down

.PHONY: dev-nuke
dev-nuke: ## Stop the dev stack and delete its volumes (destroys all ingested data)
	$(COMPOSE) down -v

.PHONY: dev-logs
dev-logs: ## Follow logs from the dev stack
	$(COMPOSE) logs -f

.PHONY: dev-ps
dev-ps: ## Show dev stack container status
	$(COMPOSE) ps

.PHONY: dev-scale
dev-scale: ## Scale collectors: make dev-scale N=5 (host ports become a range)
	$(COMPOSE_SCALE) up -d --build --wait --wait-timeout 240 --scale collector=$(or $(N),3)
	@echo
	@echo "Compose assigns ports from a range in arbitrary order:"
	@$(COMPOSE_SCALE) ps --format '  {{.Name}}\t{{.Ports}}'

.PHONY: psql
psql: ## Open a psql shell against the dev database
	$(COMPOSE) exec timescaledb psql -U logagg -d logagg

## ---- placeholders for later phases ---------------------------------------

.PHONY: proto
proto: ## Generate Go code from protobuf definitions (phase 1)
	@echo "not implemented until phase 1: api/proto has no definitions yet"

.PHONY: migrate
migrate: ## Apply database migrations (phase 1)
	@echo "not implemented until phase 1: migrations/ is empty"

.PHONY: certs
certs: ## Generate development mTLS certificates (phase 2)
	@echo "not implemented until phase 2"
