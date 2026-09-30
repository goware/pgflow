SHELL           = bash -o pipefail
TEST_FLAGS      ?= -count=1 -timeout 300s

PG_URL          ?= postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  build        - Build"
	@echo "  test         - Run tests (requires Postgres at PG_URL)"
	@echo "  lint         - Run go vet"
	@echo "  test-clean   - Clear the test cache"
	@echo "  clean        - Clear caches"

build:
	go build ./...

lint:
	go vet ./...

test:
	WORKFLOW_TEST_DATABASE_URL='$(PG_URL)' go test $(TEST_FLAGS) ./...

test-clean:
	go clean -testcache

clean:
	go clean -cache -testcache

.PHONY: all build lint test test-clean clean
