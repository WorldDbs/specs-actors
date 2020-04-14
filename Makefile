GO_BIN ?= go
all: build lint test tidy
.PHONY: all

build:
	$(GO_BIN) build ./...
.PHONY: build

test:
	$(GO_BIN) test ./...
	$(GO_BIN) test -race ./actors/migration/nv4/test
.PHONY: test

test-migration:
	$(GO_BIN) test -race ./actors/migration/nv4/test
.PHONY: test-migration

test-coverage:
	$(GO_BIN) test -coverprofile=coverage.out ./...
.PHONY: test-coverage

tidy:
	$(GO_BIN) mod tidy
.PHONY: tidy

gen:
	$(GO_BIN) run ./gen/gen.go
.PHONY: gen


# tools
toolspath:=support/tools

$(toolspath)/bin/golangci-lint: $(toolspath)/go.mod
	@mkdir -p $(dir $@)
	(cd $(toolspath); go build -tags tools -o $(@:$(toolspath)/%=%) github.com/golangci/golangci-lint/cmd/golangci-lint)


$(toolspath)/bin/no-map-range.so: $(toolspath)/go.mod
	@mkdir -p $(dir $@)
	(cd $(toolspath); go build -tags tools -buildmode=plugin -o $(@:$(toolspath)/%=%) github.com/Kubuxu/go-no-map-range/plugin)

lint: $(toolspath)/bin/golangci-lint $(toolspath)/bin/no-map-range.so
	$(toolspath)/bin/golangci-lint run ./...
.PHONY: lint
