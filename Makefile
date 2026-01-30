.PHONY: build test test-cover test-integration test-all clean run lint fmt deps install release release-dry-run release-snapshot release-check docker-build docker-up docker-down docker-test

BINARY := bin/mysql-cleanup
VERSION := $(shell git describe --tags --exact-match 2>/dev/null || echo "")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT)"

build:
	@mkdir -p bin
	go build $(LDFLAGS) -o $(BINARY) ./cmd/mysql-cleanup

test:
	go test -v -race ./internal/cleanup/...

test-cover:
	go test -v -race -coverprofile=coverage.out ./internal/cleanup/...
	go tool cover -html=coverage.out -o coverage.html

test-integration:
	go test -v -tags=integration -timeout 10m ./integration/...

test-all: test test-integration

clean:
	rm -rf bin/ coverage.out coverage.html

run: build
	./$(BINARY) --help

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...
	@which goimports > /dev/null 2>&1 && goimports -w . || echo "goimports not installed, skipping (install with: go install golang.org/x/tools/cmd/goimports@latest)"

deps:
	go mod download
	go mod tidy

install:
	go install $(LDFLAGS) ./cmd/mysql-cleanup

release:
	@echo "Creating a new release..."
	@latest=$$(git describe --tags --abbrev=0 2>/dev/null); \
	if [ -n "$$latest" ]; then \
		echo "Latest tag: $$latest"; \
		next=$$(echo "$$latest" | awk -F. '{print $$1"."$$2"."$$3+1}'); \
		read -p "Enter version [$$next]: " version; \
		version=$${version:-$$next}; \
	else \
		read -p "Enter version (e.g., v0.1.0): " version; \
	fi; \
	git tag -a $$version -m "Release $$version"; \
	git push origin $$version

release-dry-run:
	goreleaser release --clean --skip=publish

release-snapshot:
	goreleaser release --snapshot --clean

release-check:
	goreleaser check

# Docker targets
docker-build:
	docker build -t mysql-cleanup:local .

# Test database targets
docker-up:
	docker compose -f tests/docker-compose.yml up -d

docker-down:
	docker compose -f tests/docker-compose.yml down -v

docker-test: docker-up test-integration docker-down
