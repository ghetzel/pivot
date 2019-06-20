
LOCALS   := $(shell find . -type f -name '*.go')
EXAMPLES := $(wildcard examples/*)

.PHONY: test deps docs $(EXAMPLES) build
.EXPORT_ALL_VARIABLES:
GO111MODULE = on

all: deps fmt test build docs

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	go get ./...

fmt:
	go generate -x ./...
	gofmt -w $(LOCALS)
	go vet ./...
	-go mod tidy

docs:
	cd docs && make

test:
	go test -count=1 --tags json1 ./...

$(EXAMPLES):
	go build --tags json1 -o bin/example-$(notdir $(@)) $(@)/*.go

build: $(EXAMPLES)
	go build --tags json1 -i -o bin/pivot pivot/*.go
	which pivot && cp -v bin/pivot `which pivot` || true