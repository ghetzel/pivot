.PHONY: test deps

all: fmt deps build

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	@go list golang.org/x/tools/cmd/goimports || go get golang.org/x/tools/cmd/goimports
	go generate -x
	go get .

clean-bundle:
	-rm -rf public

clean:
	-rm -rf bin

fmt:
	goimports -w .
	go vet .

test:
	go test .
	go test ./dal/
	go test ./filter/
	go test ./filter/*/*
	go test ./mapper/

build: deps fmt
	go build --tags json1 -o bin/`basename ${PWD}` cli/*.go

