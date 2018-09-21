.PHONY: test deps

LOCALS :=$(shell find . -type f -name '*.go' -not -path "./vendor/*")

.EXPORT_ALL_VARIABLES:
GO111MODULE           = on

all: deps fmt test build

deps:
	go get ./...

fmt:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	@go list golang.org/x/tools/cmd/goimports || go get golang.org/x/tools/cmd/goimports
	go generate -x ./...
	goimports -w $(LOCALS)
	go vet ./...

test:
	go test --tags json1 ./...

build:
	test -d pivot && go build --tags json1 -i -o bin/pivot pivot/*.go

quickbuild: deps fmt
	test -d pivot && go build -i -o bin/pivot pivot/*.go
