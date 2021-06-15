.EXPORT_ALL_VARIABLES:
EXAMPLES       := $(wildcard examples/*)
GO111MODULE    ?= on
CGO_CFLAGS      = -I/opt/homebrew/include
CGO_LDFLAGS     = -L/opt/homebrew/lib
DOCKER_VERSION  = $(shell grep -m1 'go get github.com/ghetzel/pivot' Dockerfile | cut -d@ -f2 | tr -d v)

all: deps fmt test build docs

deps:
	@go list github.com/mjibson/esc > /dev/null || go get github.com/mjibson/esc/...
	@go get ./...

fmt:
	@go generate -x ./...
	@gofmt -w $(shell find . -type f -name '*.go')
	@go vet ./...
	@go mod tidy

docs:
	owndoc render --property rootpath=/pivot/

test:
	go test -count=1 --tags json1 ./...

$(EXAMPLES):
	go build --tags json1 -o bin/example-$(notdir $(@)) $(@)/*.go

build: $(EXAMPLES)
	go build --tags json1 -o bin/pivot cmd/pivot/*.go
	which pivot && cp -v bin/pivot `which pivot` || true

docker-build:
	@docker build -t ghetzel/pivot:$(DOCKER_VERSION) .

docker-push:
	@docker tag ghetzel/pivot:$(DOCKER_VERSION) ghetzel/pivot:latest
	@docker push ghetzel/pivot:$(DOCKER_VERSION)
	@docker push ghetzel/pivot:latest

docker: docker-build docker-push

.PHONY: test deps docs $(EXAMPLES) build docker docker-build docker-push
