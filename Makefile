.PHONY: test deps docs

LOCALS :=$(shell find . -type f -name '*.go')

.EXPORT_ALL_VARIABLES:
GO111MODULE = on

all: deps fmt test build docs

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	go get ./...
	-go mod tidy

fmt:
	go generate -x ./...
	gofmt -w $(LOCALS)
	go vet ./...

docs:
	cd docs && make

test:
	go test -count=1 --tags json1 ./...

build:
	go build --tags json1 -i -o bin/pivot pivot/*.go
	which pivot && cp -v bin/pivot `which pivot` || true