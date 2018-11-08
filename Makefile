.PHONY: test deps

LOCALS :=$(shell find . -type f -name '*.go' -not -path "./vendor/*")

.EXPORT_ALL_VARIABLES:
GO111MODULE = on

all: deps fmt test build

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	go get ./...

fmt:
	go generate -x ./...
	gofmt -w $(LOCALS)
	go vet ./...

test:
	go test --tags json1 ./...

build:
	test -d pivot && go build --tags json1 -i -o bin/pivot pivot/*.go
	which pivot && cp -v bin/pivot `which pivot` || true