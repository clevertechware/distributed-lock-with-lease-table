GOLANGCI_LINT_VERSION ?= v2.6.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: all
all: fmt vet lint test

.PHONY: fmt
fmt:
	$(GOLANGCI_LINT) fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint:
	$(GOLANGCI_LINT) run ./...

# The suite starts a throwaway PostgreSQL container, so a running Docker daemon is required.
.PHONY: test
test:
	go test ./... -race

.PHONY: tidy
tidy:
	go mod tidy

# The throwaway PostgreSQL the cmd/replica demonstration talks to, on the image the tests use.
.PHONY: demo-db
demo-db:
	docker run -d --rm --name lease-demo -p 5432:5432 \
		-e POSTGRES_USER=demo -e POSTGRES_PASSWORD=demo -e POSTGRES_DB=demo postgres:17-alpine

.PHONY: demo-db-down
demo-db-down:
	docker rm -f lease-demo
