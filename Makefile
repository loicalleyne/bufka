
.PHONY: build clean test run tui help

SERVICE_NAME=bufka

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build: pkg=github.com/loicalleyne/bufka/pkg/version
build: ldflags = -X $(pkg).version=$(shell git describe --tags --always --dirty)
build: ldflags += -X $(pkg).commit=$(shell git rev-parse HEAD)
build: ldflags += -X $(pkg).buildDate=$(shell date -Iseconds)
tags: "-tags embedfull ./..."

build: ## Build the application
	@echo "Building $(SERVICE_NAME)..."
	@CGO_ENABLED=1 go build -o $(SERVICE_NAME) -ldflags "$(ldflags)" $(tags) main.go

# ENV_FILE is passed to bufka explicitly (it never reads ./.env on its own).
ENV_FILE ?= .env
env_flag = $(if $(wildcard $(ENV_FILE)),--env-file $(ENV_FILE))

run: ## Run the application (with ENV_FILE, default .env, when present)
	@echo "Running $(SERVICE_NAME)..."
	@CGO_ENABLED=1 go run main.go $(env_flag) run

tui: ## Run the TUI (with ENV_FILE, default .env, when present)
	@CGO_ENABLED=1 go run main.go $(env_flag) tui

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -f $(SERVICE_NAME) coverage.out

test: ## Run tests
	@echo "Running tests..."
	@go test -v -race -coverprofile=coverage.out ./...

test-coverage: test ## Run tests with coverage report
	@go tool cover -html=coverage.out

fmt: ## Format code
	@echo "Formatting code..."
	@go fmt ./...

lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run ./...

mod-tidy: ## Tidy go modules
	@echo "Tidying modules..."
	@go mod tidy

.DEFAULT_GOAL := help
