GO ?= go
PROTOC ?= protoc
PROTOC_GEN_GO ?= protoc-gen-go
PROTOC_GEN_GO_GRPC ?= protoc-gen-go-grpc

.PHONY: all generate build test race vet clean

all: test build

generate:
	$(PROTOC) --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC) \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/capmesh/v1/capture.proto

build:
	$(GO) build -trimpath -o bin/capmesh-server ./cmd/capmesh-server
	$(GO) build -trimpath -o bin/capmesh-agent ./cmd/capmesh-agent
	$(GO) build -trimpath -o bin/capmesh-client ./cmd/capmesh-client

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

clean:
	$(GO) clean
