.PHONY: test deps

all: fmt deps build bundle

deps:
	go get .

clean-bundle:
	-rm -rf public

clean:
	-rm -rf bin

fmt:
	gofmt -w .

test:
	go test .
	go test ./dal/
	go test ./filter/
	go test ./filter/*/*
	go test ./mapper/

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

