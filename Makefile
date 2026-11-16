.PHONY: build run test clean docker-build docker-up docker-down

BINARY ?= seedcache
BUILD_DIR ?= bin
ARGS ?= --config config.yaml daemon

build:
	mkdir -p $(BUILD_DIR)
	go build -trimpath -o $(BUILD_DIR)/$(BINARY) ./cmd/seedcache

run:
	go run ./cmd/seedcache $(ARGS)

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)

docker-build:
	docker compose build

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down
