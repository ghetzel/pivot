.PHONY: test deps

LOCALS :=$(shell find . -type f -name '*.go' -not -path "./vendor/*")

.EXPORT_ALL_VARIABLES:
GO111MODULE = on

all: deps fmt test build

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	go get ./...
	go vet ./...
	go generate -x ./...

fmt:
	gofmt -w $(LOCALS)

test:
	go test -count=1 --tags json1 ./...

build:
	test -d pivot && go build --tags json1 -i -o bin/pivot pivot/*.go