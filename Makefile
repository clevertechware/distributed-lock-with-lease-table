GOLANGCI_LINT_VERSION ?= v2.6.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: all fmt vet lint test tidy demo-db demo-db-down

all: fmt vet lint test

fmt:
	$(GOLANGCI_LINT) fmt ./...

vet:
	go vet ./...

lint:
	$(GOLANGCI_LINT) run ./...

# The suite starts a throwaway PostgreSQL container, so a running Docker daemon is required.
test:
	go test ./... -race

tidy:
	go mod tidy

demo-db:
	docker compose up -d --wait

demo-db-down:
	docker compose down
