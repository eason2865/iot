.PHONY: all test fmt fmt-check build clean helm-local proto

BINS := management-api iot-core demo telemetry-ingestor device-worker dlq-replay
GOOS ?= linux
GOARCH ?= arm64
CGO_ENABLED ?= 0

# Pinned to the versions recorded in proto/core/v1/*.pb.go headers; bumping
# either regenerates the header comment and fails the CI drift check.
PROTOC_VERSION ?= 36.2
PROTOC_GEN_GO_VERSION ?= v1.36.8
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

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

proto:
	protoc --go_out=. --go_opt=module=iot --go-grpc_out=. --go-grpc_opt=module=iot proto/core/v1/core.proto

proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
