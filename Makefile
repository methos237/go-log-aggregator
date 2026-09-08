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

# Pinned codegen toolchain. These are not go.mod dependencies: nothing in the
# built binaries imports them, and adding them would put buf's dependency tree
# into every `go mod download`.
BUF_VERSION                := v1.72.0
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

# Host DSN for migrations run from the host rather than from inside a container.
# Matches the port deploy/docker-compose.yml publishes on loopback.
DB_DSN ?= postgres://logagg:logagg@127.0.0.1:5432/logagg?sslmode=disable

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

## ---- protobuf -------------------------------------------------------------

.PHONY: proto-tools
proto-tools: ## Install pinned protoc plugins into bin/
	@mkdir -p $(BIN_DIR)
	GOBIN=$(CURDIR)/$(BIN_DIR) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(CURDIR)/$(BIN_DIR) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: proto
proto: proto-tools ## Generate Go code from protobuf definitions
	PATH="$(CURDIR)/$(BIN_DIR):$$PATH" go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate

.PHONY: proto-lint
proto-lint: ## Lint protobuf definitions
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) lint

.PHONY: proto-check
proto-check: proto ## Fail if the committed generated code is stale
	@if ! git diff --quiet -- api/proto; then \
		echo "generated protobuf code is stale; run 'make proto' and commit the result"; \
		git diff --stat -- api/proto; exit 1; \
	fi

## ---- database -------------------------------------------------------------

.PHONY: migrate
migrate: ## Apply database migrations against DB_DSN
	go run ./cmd/collector -migrate -db-dsn "$(DB_DSN)"

.PHONY: migrate-status
migrate-status: ## Print the applied schema version of DB_DSN
	go run ./cmd/collector -migrate-status -db-dsn "$(DB_DSN)"

.PHONY: migrate-down
migrate-down: ## Roll every migration back (destroys all data in DB_DSN)
	go run ./cmd/collector -migrate-down -db-dsn "$(DB_DSN)"

.PHONY: test-integration
test-integration: ## Run integration tests (needs Docker, or LOGAGG_TEST_DB_DSN)
	go test -tags=integration -timeout 20m ./test/integration/...

## ---- tls ------------------------------------------------------------------

# Development mTLS material. CERT_DIR is gitignored; see .gitignore.
#
# These are for `make dev` and for local experiments, nothing else. A real
# deployment gets certificates from a CA that can revoke them, with a rotation
# story and keys that were never on a laptop.
CERT_DIR  ?= certs
CERT_DAYS ?= 365
CERT_SANS ?= DNS:localhost,DNS:collector,DNS:agent,IP:127.0.0.1,IP:::1

.PHONY: certs
certs: ## Generate development mTLS certificates into certs/ (gitignored)
	@if [[ -f "$(CERT_DIR)/ca.pem" ]]; then \
		echo "$(CERT_DIR)/ca.pem already exists; run 'make certs-clean' first"; exit 1; \
	fi
	@mkdir -p $(CERT_DIR)
	@umask 077; \
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	printf '%s\n' \
		'[req]' 'distinguished_name = dn' 'prompt = no' \
		'[dn]' 'CN = logagg-dev-ca' \
		'[ext]' 'basicConstraints = critical,CA:TRUE,pathlen:0' \
		'keyUsage = critical,keyCertSign,cRLSign' > "$$tmp/ca.cnf"; \
	printf '%s\n' \
		'[req]' 'distinguished_name = dn' 'prompt = no' \
		'[dn]' 'CN = collector' \
		'[ext]' 'basicConstraints = critical,CA:FALSE' \
		'keyUsage = critical,digitalSignature,keyEncipherment' \
		'extendedKeyUsage = serverAuth' \
		'subjectAltName = $(CERT_SANS)' > "$$tmp/server.cnf"; \
	printf '%s\n' \
		'[req]' 'distinguished_name = dn' 'prompt = no' \
		'[dn]' 'CN = agent' \
		'[ext]' 'basicConstraints = critical,CA:FALSE' \
		'keyUsage = critical,digitalSignature' \
		'extendedKeyUsage = clientAuth' > "$$tmp/client.cnf"; \
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -noenc \
		-days $(CERT_DAYS) -config "$$tmp/ca.cnf" -extensions ext \
		-keyout $(CERT_DIR)/ca-key.pem -out $(CERT_DIR)/ca.pem 2>/dev/null; \
	for name in server client; do \
		openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -noenc \
			-config "$$tmp/$$name.cnf" \
			-keyout $(CERT_DIR)/$$name-key.pem -out "$$tmp/$$name.csr" 2>/dev/null; \
		openssl x509 -req -in "$$tmp/$$name.csr" -days $(CERT_DAYS) \
			-CA $(CERT_DIR)/ca.pem -CAkey $(CERT_DIR)/ca-key.pem -set_serial $$RANDOM$$RANDOM \
			-extfile "$$tmp/$$name.cnf" -extensions ext \
			-out $(CERT_DIR)/$$name.pem 2>/dev/null; \
	done
	@chmod 600 $(CERT_DIR)/*-key.pem
	@echo "development certificates written to $(CERT_DIR)/ (gitignored, dev only):"
	@echo "  ca.pem          trust root for both sides"
	@echo "  server.pem/-key collector, SAN $(CERT_SANS)"
	@echo "  client.pem/-key agent"
	@echo
	@echo "enable mTLS on the collector:"
	@echo "  LOGAGG_INGEST_TLS_CERT_FILE=$(CERT_DIR)/server.pem"
	@echo "  LOGAGG_INGEST_TLS_KEY_FILE=$(CERT_DIR)/server-key.pem"
	@echo "  LOGAGG_INGEST_TLS_CLIENT_CA_FILE=$(CERT_DIR)/ca.pem"

.PHONY: certs-clean
certs-clean: ## Delete the development certificates
	rm -rf $(CERT_DIR)

.PHONY: certs-verify
certs-verify: ## Show what the development certificates actually say
	@openssl verify -CAfile $(CERT_DIR)/ca.pem $(CERT_DIR)/server.pem $(CERT_DIR)/client.pem
	@for f in server client; do \
		echo "--- $$f"; \
		openssl x509 -in $(CERT_DIR)/$$f.pem -noout -subject -dates -ext subjectAltName,extendedKeyUsage; \
	done
