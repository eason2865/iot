.PHONY: all test fmt fmt-check build clean helm-local

BINS := admin core-rpc demo ingress worker
GOOS ?= linux
GOARCH ?= arm64
CGO_ENABLED ?= 0

all: fmt-check test build

test:
	go test ./...

fmt:
	gofmt -w $$(find . -path ./.git -prune -o -path ./outputs -prune -o -name '*.go' -print)

fmt-check:
	@test -z "$$(gofmt -l $$(find . -path ./.git -prune -o -path ./outputs -prune -o -name '*.go' -print))"

build:
	@mkdir -p bin
	@for bin in $(BINS); do \
		echo "building $$bin"; \
		GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) go build -o "bin/$$bin" "./cmd/$$bin"; \
	done

clean:
	rm -rf bin coverage.out coverage.html

helm-local:
	scripts/helm-deploy-local.sh
