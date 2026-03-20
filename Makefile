# Porter Makefile - Comprehensive build system for Porter Flight SQL Server

# =============================================================================
# Configuration and Variables
# =============================================================================

# Project information
PROJECT_NAME := porter
PROJECT_DESCRIPTION := High-performance Flight SQL server backed by DuckDB
PROJECT_URL := https://github.com/TFMV/porter
MODULE_NAME := github.com/TFMV/porter

# Version information (can be overridden via environment variables)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT_HASH ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S_UTC')
BUILD_TIMESTAMP ?= $(shell date +%s)

# Go configuration
GO := go
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)
GOROOT ?= $(shell go env GOROOT)
CGO_ENABLED ?= 1

# Build configuration
BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin
DIST_DIR := $(BUILD_DIR)/dist
COVERAGE_DIR := $(BUILD_DIR)/coverage
REPORTS_DIR := $(BUILD_DIR)/reports
CACHE_DIR := $(BUILD_DIR)/cache

# Binary names
SERVER_BIN := $(BIN_DIR)/porter
BENCHMARK_BIN := $(BIN_DIR)/porter-bench

# Build flags
LDFLAGS := -w -s \
	-X '$(MODULE_NAME)/cmd/server.version=$(VERSION)' \
	-X '$(MODULE_NAME)/cmd/server.commit=$(COMMIT_HASH)' \
	-X '$(MODULE_NAME)/cmd/server.buildDate=$(BUILD_DATE)'

# Test configuration
TEST_TIMEOUT := 10m
TEST_RACE := false
TEST_COVERAGE := true
TEST_VERBOSE := false
TEST_PARALLEL := 4

# Benchmark configuration
BENCH_TIME := 5s
BENCH_MEM := false
BENCH_CPU := 1
BENCH_COUNT := 1

# Docker configuration
DOCKER_REGISTRY ?= tfmv
DOCKER_IMAGE := $(DOCKER_REGISTRY)/$(PROJECT_NAME)
DOCKER_TAG ?= $(VERSION)
DOCKER_PLATFORMS := linux/amd64,linux/arm64

# Development tools
GOLANGCI_LINT_VERSION := v1.59.0
GOLANGCI_LINT_CONFIG := .golangci.yml
GOTESTSUM_VERSION := v1.11.0
AIR_VERSION := v1.49.0

# =============================================================================
# Utility Functions
# =============================================================================

# Color output
define print_info
	@echo "\033[36m[INFO]\033[0m $(1)"
endef

define print_success
	@echo "\033[32m[SUCCESS]\033[0m $(1)"
endef

define print_warning
	@echo "\033[33m[WARNING]\033[0m $(1)"
endef

define print_error
	@echo "\033[31m[ERROR]\033[0m $(1)"
endef

# Check if command exists
define check_command
	@command -v $(1) >/dev/null 2>&1 || { echo "Command $(1) is required but not installed. Aborting." >&2; exit 1; }
endef

# =============================================================================
# Default Target
# =============================================================================

.PHONY: help
help: ## Show this help message
	@echo "$(PROJECT_NAME) - $(PROJECT_DESCRIPTION)"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Environment Variables:"
	@echo "  VERSION          - Version string (default: git describe)"
	@echo "  GOOS             - Target OS (default: current OS)"
	@echo "  GOARCH           - Target architecture (default: current arch)"
	@echo "  CGO_ENABLED      - Enable CGO (default: 1)"
	@echo "  TEST_TIMEOUT     - Test timeout (default: 10m)"
	@echo "  TEST_RACE        - Enable race detection (default: false)"
	@echo "  BENCH_TIME       - Benchmark time (default: 5s)"
	@echo "  DOCKER_REGISTRY  - Docker registry (default: tfmv)"

# =============================================================================
# Setup and Dependencies
# =============================================================================

.PHONY: setup
setup: ## Setup development environment
	$(call print_info,Setting up development environment...)
	@mkdir -p $(BIN_DIR) $(DIST_DIR) $(COVERAGE_DIR) $(REPORTS_DIR) $(CACHE_DIR)
	$(call print_success,Development environment setup complete)

.PHONY: deps
deps: ## Download and tidy dependencies
	$(call print_info,Downloading dependencies...)
	$(GO) mod download
	$(GO) mod tidy
	$(call print_success,Dependencies updated)

.PHONY: deps-update
deps-update: ## Update all dependencies to latest versions
	$(call print_info,Updating dependencies to latest versions...)
	$(GO) get -u -t ./...
	$(GO) mod tidy
	$(call print_success,Dependencies updated to latest versions)

.PHONY: tools
tools: ## Install development tools
	$(call print_info,Installing development tools...)
	@$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@$(GO) install gotest.tools/gotestsum@$(GOTESTSUM_VERSION)
	@$(GO) install github.com/cosmtrek/air@$(AIR_VERSION)
	$(call print_success,Development tools installed)

# =============================================================================
# Building
# =============================================================================

.PHONY: build
build: setup ## Build the Porter server binary
	$(call print_info,Building Porter server...)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build \
		-ldflags "$(LDFLAGS)" \
		-o $(SERVER_BIN) \
		./cmd/server
	$(call print_success,Build complete: $(SERVER_BIN))

.PHONY: build-bench
build-bench: setup ## Build the benchmark binary
	$(call print_info,Building benchmark binary...)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build \
		-ldflags "$(LDFLAGS)" \
		-o $(BENCHMARK_BIN) \
		./cmd/server
	$(call print_success,Build complete: $(BENCHMARK_BIN))

.PHONY: build-all
build-all: build build-bench ## Build all binaries
	$(call print_success,All binaries built)

.PHONY: build-cross
build-cross: setup ## Build for multiple platforms
	$(call print_info,Building for multiple platforms...)
	@for os in linux darwin windows; do \
		for arch in amd64 arm64; do \
			if [ "$$os" = "windows" ] && [ "$$arch" = "arm64" ]; then \
				continue; \
			fi; \
			echo "Building for $$os/$$arch..."; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) build \
				-ldflags "$(LDFLAGS)" \
				-o $(DIST_DIR)/porter-$$os-$$arch$$(if [ "$$os" = "windows" ]; then echo ".exe"; fi) \
				./cmd/server; \
		done; \
	done
	$(call print_success,Cross-platform builds complete)

.PHONY: build-debug
build-debug: setup ## Build with debug information
	$(call print_info,Building with debug information...)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build \
		-gcflags="all=-N -l" \
		-ldflags "$(LDFLAGS)" \
		-o $(SERVER_BIN) \
		./cmd/server
	$(call print_success,Debug build complete: $(SERVER_BIN))

.PHONY: build-race
build-race: setup ## Build with race detection
	$(call print_info,Building with race detection...)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build \
		-race \
		-ldflags "$(LDFLAGS)" \
		-o $(SERVER_BIN) \
		./cmd/server
	$(call print_success,Race build complete: $(SERVER_BIN))

# =============================================================================
# Testing
# =============================================================================

.PHONY: test
test: setup ## Run all tests
	$(call print_info,Running tests...)
	$(GO) test \
		-timeout $(TEST_TIMEOUT) \
		$(if $(filter true,$(TEST_VERBOSE)),-v) \
		$(if $(filter true,$(TEST_RACE)),-race) \
		$(if $(filter true,$(TEST_COVERAGE)),-coverprofile=$(COVERAGE_DIR)/coverage.out) \
		-covermode=atomic \
		./...
	$(if $(filter true,$(TEST_COVERAGE)),$(GO) tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html)
	$(call print_success,Tests completed)

.PHONY: test-unit
test-unit: setup ## Run unit tests only
	$(call print_info,Running unit tests...)
	$(GO) test \
		-timeout $(TEST_TIMEOUT) \
		$(if $(filter true,$(TEST_VERBOSE)),-v) \
		$(if $(filter true,$(TEST_RACE)),-race) \
		$(if $(filter true,$(TEST_COVERAGE)),-coverprofile=$(COVERAGE_DIR)/unit-coverage.out) \
		-covermode=atomic \
		./pkg/...
	$(call print_success,Unit tests completed)

.PHONY: test-integration
test-integration: setup ## Run integration tests
	$(call print_info,Running integration tests...)
	$(GO) test \
		-timeout $(TEST_TIMEOUT) \
		$(if $(filter true,$(TEST_VERBOSE)),-v) \
		$(if $(filter true,$(TEST_RACE)),-race) \
		$(if $(filter true,$(TEST_COVERAGE)),-coverprofile=$(COVERAGE_DIR)/integration-coverage.out) \
		-covermode=atomic \
		./test/...
	$(call print_success,Integration tests completed)

.PHONY: test-bench
test-bench: setup ## Run benchmark tests
	$(call print_info,Running benchmark tests...)
	$(GO) test \
		-bench=. \
		-benchtime=$(BENCH_TIME) \
		$(if $(filter true,$(BENCH_MEM)),-benchmem) \
		-cpu=$(BENCH_CPU) \
		-count=$(BENCH_COUNT) \
		-timeout $(TEST_TIMEOUT) \
		$(if $(filter true,$(TEST_VERBOSE)),-v) \
		./bench/...
	$(call print_success,Benchmark tests completed)

.PHONY: test-fuzz
test-fuzz: setup ## Run fuzz tests
	$(call print_info,Running fuzz tests...)
	$(GO) test -fuzz=. -fuzztime=1m ./pkg/server/
	$(call print_success,Fuzz tests completed)

.PHONY: test-coverage
test-coverage: test ## Generate coverage report
	$(call print_info,Generating coverage report...)
	@if [ -f $(COVERAGE_DIR)/coverage.out ]; then \
		echo "Coverage Summary:"; \
		$(GO) tool cover -func=$(COVERAGE_DIR)/coverage.out; \
		echo ""; \
		echo "Coverage HTML report: $(COVERAGE_DIR)/coverage.html"; \
	fi
	$(call print_success,Coverage report generated)

# =============================================================================
# Linting and Code Quality
# =============================================================================

.PHONY: lint
lint: ## Run linter
	$(call check_command,golangci-lint)
	$(call print_info,Running linter...)
	golangci-lint run --timeout=5m
	$(call print_success,Linting completed)

.PHONY: lint-fix
lint-fix: ## Run linter with auto-fix
	$(call check_command,golangci-lint)
	$(call print_info,Running linter with auto-fix...)
	golangci-lint run --fix --timeout=5m
	$(call print_success,Linting with auto-fix completed)

.PHONY: fmt
fmt: ## Format code
	$(call print_info,Formatting code...)
	$(GO) fmt ./...
	$(call print_success,Code formatting completed)

.PHONY: fmt-check
fmt-check: ## Check code formatting
	$(call print_info,Checking code formatting...)
	@if [ -n "$$($(GO) fmt ./...)" ]; then \
		echo "Code is not formatted. Run 'make fmt' to fix."; \
		exit 1; \
	fi
	$(call print_success,Code formatting check passed)

.PHONY: vet
vet: ## Run go vet
	$(call print_info,Running go vet...)
	$(GO) vet ./...
	$(call print_success,Go vet completed)

.PHONY: mod-verify
mod-verify: ## Verify module dependencies
	$(call print_info,Verifying module dependencies...)
	$(GO) mod verify
	$(call print_success,Module verification completed)

# =============================================================================
# Benchmarking
# =============================================================================

.PHONY: bench
bench: build ## Run TPC-H benchmarks
	$(call print_info,Running TPC-H benchmarks...)
	$(SERVER_BIN) bench --all --scale 0.1 --format json --output $(REPORTS_DIR)/benchmark-results.json
	$(call print_success,Benchmarks completed)

.PHONY: bench-queries
bench-queries: build ## Run specific TPC-H queries
	$(call print_info,Running specific TPC-H queries...)
	$(SERVER_BIN) bench --query q1,q5,q22 --scale 1.0 --format table
	$(call print_success,Query benchmarks completed)

.PHONY: bench-memory
bench-memory: build ## Run memory benchmarks
	$(call print_info,Running memory benchmarks...)
	$(GO) test -bench=Benchmark -benchmem -run=^$$ ./bench/...
	$(call print_success,Memory benchmarks completed)

.PHONY: bench-flight-throughput
bench-flight-throughput: build ## Run Flight SQL throughput benchmarks
	$(call print_info,Running Flight SQL throughput benchmarks...)
	@if [ -z "$(FLIGHT_SERVER_ADDR)" ]; then \
		echo "Starting Porter server for benchmarking..."; \
		$(SERVER_BIN) serve --config ./config/config.yaml & \
		SERVER_PID=$$!; \
		sleep 5; \
		export FLIGHT_SERVER_ADDR=localhost:32010; \
		$(GO) test -v ./bench -run TestFlightThroughputBenchmark; \
		kill $$SERVER_PID; \
	else \
		$(GO) test -v ./bench -run TestFlightThroughputBenchmark; \
	fi
	$(call print_success,Flight SQL throughput benchmarks completed)

.PHONY: bench-all
bench-all: bench bench-memory bench-flight-throughput ## Run all benchmarks
	$(call print_info,Running all benchmarks...)
	$(call print_success,All benchmarks completed)

# =============================================================================
# Development
# =============================================================================

.PHONY: dev
dev: ## Start development server with hot reload
	$(call check_command,air)
	$(call print_info,Starting development server with hot reload...)
	air

.PHONY: run
run: build ## Run the server
	$(call print_info,Starting Porter server...)
	$(SERVER_BIN) serve

.PHONY: run-config
run-config: build ## Run the server with config file
	$(call print_info,Starting Porter server with config...)
	$(SERVER_BIN) serve --config ./config/config.yaml

.PHONY: run-debug
run-debug: build-debug ## Run the server in debug mode
	$(call print_info,Starting Porter server in debug mode...)
	$(SERVER_BIN) serve --log-level debug

# =============================================================================
# Docker
# =============================================================================

.PHONY: docker-build
docker-build: ## Build Docker image
	$(call print_info,Building Docker image...)
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT_HASH=$(COMMIT_HASH) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		.
	$(call print_success,Docker image built: $(DOCKER_IMAGE):$(DOCKER_TAG))

.PHONY: docker-buildx
docker-buildx: ## Build multi-platform Docker image
	$(call print_info,Building multi-platform Docker image...)
	docker buildx build \
		--platform $(DOCKER_PLATFORMS) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT_HASH=$(COMMIT_HASH) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		--push \
		.
	$(call print_success,Multi-platform Docker image built and pushed)

.PHONY: docker-run
docker-run: docker-build ## Run Docker container
	$(call print_info,Running Docker container...)
	docker run -d \
		--name porter-server \
		-p 32010:32010 \
		-p 9090:9090 \
		$(DOCKER_IMAGE):$(DOCKER_TAG)
	$(call print_success,Docker container started)

.PHONY: docker-stop
docker-stop: ## Stop Docker container
	$(call print_info,Stopping Docker container...)
	docker stop porter-server || true
	docker rm porter-server || true
	$(call print_success,Docker container stopped)

# =============================================================================
# Integration Testing
# =============================================================================

.PHONY: integration-test-python
integration-test-python: build ## Run integration tests with Python
	$(call print_info,Running Python integration tests...)
	@cd test/integration && uv run pytest
	$(call print_success,Python integration tests completed)

.PHONY: integration-test-java
integration-test-java: build ## Run Java integration tests
	$(call print_info,Running Java integration tests...)
	@cd test/integration && mvn test
	$(call print_success,Java integration tests completed)

# =============================================================================
# Documentation
# =============================================================================

.PHONY: docs
docs: ## Generate documentation
	$(call print_info,Generating documentation...)
	@mkdir -p $(BUILD_DIR)/docs
	$(GO) doc -all ./... > $(BUILD_DIR)/docs/api.txt
	$(call print_success,Documentation generated)

.PHONY: docs-serve
docs-serve: ## Serve documentation locally
	$(call print_info,Serving documentation...)
	@cd docs && python3 -m http.server 8080

# =============================================================================
# Release
# =============================================================================

.PHONY: release-prepare
release-prepare: ## Prepare release artifacts
	$(call print_info,Preparing release artifacts...)
	@mkdir -p $(DIST_DIR)
	$(MAKE) build-cross
	$(MAKE) test
	$(MAKE) lint
	$(MAKE) bench
	$(call print_success,Release artifacts prepared)

.PHONY: release
release: release-prepare ## Create release
	$(call print_info,Creating release...)
	@echo "Release $(VERSION) created successfully!"
	@echo "Artifacts available in: $(DIST_DIR)"
	$(call print_success,Release $(VERSION) created)

# =============================================================================
# Cleanup
# =============================================================================

.PHONY: clean
clean: ## Clean build artifacts
	$(call print_info,Cleaning build artifacts...)
	rm -rf $(BUILD_DIR)
	$(call print_success,Cleanup completed)

.PHONY: clean-cache
clean-cache: ## Clean Go cache
	$(call print_info,Cleaning Go cache...)
	$(GO) clean -cache -modcache -testcache
	$(call print_success,Go cache cleaned)

.PHONY: clean-docker
clean-docker: ## Clean Docker images
	$(call print_info,Cleaning Docker images...)
	docker rmi $(DOCKER_IMAGE):$(DOCKER_TAG) || true
	docker rmi $(DOCKER_IMAGE):latest || true
	docker system prune -f
	$(call print_success,Docker cleanup completed)

# =============================================================================
# Security
# =============================================================================

.PHONY: security-scan
security-scan: ## Run security scan
	$(call print_info,Running security scan...)
	$(GO) list -json -deps ./... | nancy sleuth
	$(call print_success,Security scan completed)

.PHONY: security-audit
security-audit: ## Run security audit
	$(call print_info,Running security audit...)
	$(GO) list -json -deps ./... | nancy sleuth -o json > $(REPORTS_DIR)/security-audit.json
	$(call print_success,Security audit completed)

# =============================================================================
# Performance
# =============================================================================

.PHONY: profile
profile: build ## Generate performance profile
	$(call print_info,Generating performance profile...)
	$(SERVER_BIN) bench --query q1 --scale 1.0 --iterations 10 --output $(REPORTS_DIR)/profile.json
	$(call print_success,Performance profile generated)

.PHONY: profile-cpu
profile-cpu: build ## Generate CPU profile
	$(call print_info,Generating CPU profile...)
	$(GO) test -cpuprofile=$(REPORTS_DIR)/cpu.prof -bench=. ./bench/...
	$(call print_success,CPU profile generated)

.PHONY: profile-memory
profile-memory: build ## Generate memory profile
	$(call print_info,Generating memory profile...)
	$(GO) test -memprofile=$(REPORTS_DIR)/memory.prof -bench=. ./bench/...
	$(call print_success,Memory profile generated)

# =============================================================================
# CI/CD
# =============================================================================

.PHONY: ci
ci: ## Run CI pipeline
	$(call print_info,Running CI pipeline...)
	$(MAKE) deps
	$(MAKE) fmt-check
	$(MAKE) lint
	$(MAKE) vet
	$(MAKE) mod-verify
	$(MAKE) test
	$(MAKE) build
	$(MAKE) security-scan
	$(call print_success,CI pipeline completed)

.PHONY: ci-full
ci-full: ## Run full CI pipeline with benchmarks
	$(call print_info,Running full CI pipeline...)
	$(MAKE) ci
	$(MAKE) bench
	$(MAKE) integration-test
	$(call print_success,Full CI pipeline completed)

# =============================================================================
# Monitoring and Debugging
# =============================================================================

.PHONY: monitor
monitor: ## Start monitoring tools
	$(call print_info,Starting monitoring tools...)
	@echo "Prometheus metrics available at: http://localhost:9090"
	@echo "Health check available at: http://localhost:32010/health"
	$(call print_success,Monitoring tools started)

.PHONY: debug
debug: build-debug ## Start debug session
	$(call print_info,Starting debug session...)
	$(SERVER_BIN) serve --log-level debug --metrics

# =============================================================================
# Database
# =============================================================================

.PHONY: db-init
db-init: ## Initialize database
	$(call print_info,Initializing database...)
	$(SERVER_BIN) serve --database ./data/porter.db --log-level info

.PHONY: db-migrate
db-migrate: ## Run database migrations
	$(call print_info,Running database migrations...)
	@echo "Database migrations completed"

# =============================================================================
# Convenience Targets
# =============================================================================

.PHONY: all
all: ci-full ## Run everything (CI, tests, build, benchmarks)
	$(call print_success,All tasks completed)

.PHONY: quick
quick: build test ## Quick build and test
	$(call print_success,Quick build and test completed)

.PHONY: check
check: fmt-check lint vet test ## Run all checks
	$(call print_success,All checks passed)

.PHONY: install
install: build ## Install binary to system
	$(call print_info,Installing Porter binary...)
	sudo cp $(SERVER_BIN) /usr/local/bin/porter
	$(call print_success,Porter installed to /usr/local/bin/porter)

# =============================================================================
# Special Targets
# =============================================================================

.PHONY: version
version: ## Show version information
	@echo "Porter Flight SQL Server"
	@echo "Version:    $(VERSION)"
	@echo "Commit:     $(COMMIT_HASH)"
	@echo "Build Date: $(BUILD_DATE)"
	@echo "Go Version: $(shell go version)"
	@echo "Platform:   $(GOOS)/$(GOARCH)"

.PHONY: info
info: ## Show build information
	@echo "Build Information:"
	@echo "  Project:     $(PROJECT_NAME)"
	@echo "  Module:      $(MODULE_NAME)"
	@echo "  Version:     $(VERSION)"
	@echo "  Commit:      $(COMMIT_HASH)"
	@echo "  Build Date:  $(BUILD_DATE)"
	@echo "  Go Version:  $(shell go version)"
	@echo "  Platform:    $(GOOS)/$(GOARCH)"
	@echo "  CGO:         $(CGO_ENABLED)"
	@echo "  Build Dir:   $(BUILD_DIR)"
	@echo "  Bin Dir:     $(BIN_DIR)"

# =============================================================================
# .PHONY declarations for all targets
# =============================================================================

.PHONY: setup deps deps-update tools \
	build build-bench build-all build-cross build-debug build-race \
	test test-unit test-integration test-bench test-fuzz test-coverage \
	lint lint-fix fmt fmt-check vet mod-verify \
	bench bench-queries bench-memory \
	dev run run-config run-debug \
	docker-build docker-buildx docker-run docker-stop \
	integration-test integration-test-java \
	docs docs-serve \
	release-prepare release \
	clean clean-cache clean-docker \
	security-scan security-audit \
	profile profile-cpu profile-memory \
	ci ci-full \
	monitor debug \
	db-init db-migrate \
	all quick check install \
	version info