BIN := bin/gossipd
GO  ?= go

.PHONY: build test race lint clean up down status demo

build:
	$(GO) build -trimpath -ldflags "-s -w" -o $(BIN) ./cmd/gossipd

test:
	$(GO) test ./... -count=1

race:
	$(GO) test ./... -race -count=1

vet:
	$(GO) vet ./...

clean:
	rm -rf bin /tmp/gossip-go-cluster

up: build
	./scripts/cluster.sh up 5

status:
	./scripts/cluster.sh status

down:
	./scripts/cluster.sh down

demo: up
	@sleep 6
	@echo "\n--- converged cluster ---"
	@./scripts/cluster.sh status
	@echo "\n--- killing node 5 ---"
	@./scripts/cluster.sh kill 5
	@sleep 10
	@echo "\n--- 10s later: phi climbing, not yet convicted ---"
	@./scripts/cluster.sh status
	@echo "\n(wait ~20s total for conviction; then: make down)"
