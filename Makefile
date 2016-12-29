.PHONY: test

all: vendor fmt build bundle

update:
	-rm -rf vendor
	govend -uvl

vendor:
	go list github.com/govend/govend
	govend -v -l

clean-bundle:
	-rm -rf public

clean:
	-rm -rf vendor bin

fmt:
	gofmt -w .

test: vendor
	go test -v .
	go test -v ./dal/
	go test -v ./filter/
	go test -v ./filter/*/*

bundle: clean-bundle
	@echo "Bundling static resources under ./public/"
	@test -d public && rm -rf public || true
	@mkdir public
	@cp -R static/* public/
	@mkdir public/res
	@for backend in backends/*; do \
		if [ -d "$${backend}/resources" ]; then \
			mkdir public/res/`basename "$${backend}"`; \
			cp -R $${backend}/resources/* public/res/`basename "$${backend}"`; \
		fi \
	done

build: fmt
	go build -o bin/`basename ${PWD}` cli/*.go

