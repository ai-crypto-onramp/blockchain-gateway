.PHONY: build test test-integration lint coverage run docker-build docker-run docker-up docker-down e2e-smoke clean migrate-up migrate-down

build:
	go build -o bin/blockchain-gateway ./cmd/server

test:
	go test ./cmd/... ./internal/... -race -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/...

test-integration:
	docker compose up -d postgres redis
	@echo "Waiting for Postgres + Redis to be healthy..."
	@sleep 5
	DB_URL=postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable \
	REDIS_URL=redis://localhost:6379/0 \
	CHAINS_SUPPORTED=ethereum \
	RPC_URLS_ETHEREUM=http://localhost:8545 \
	FINALITY_BLOCKS_ETHEREUM=64 \
	go test ./cmd/... ./internal/... -race -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/... -tags=integration
	docker compose down

lint:
	golangci-lint run

coverage: test
	go tool cover -func=coverage.out | tail -1

run:
	go run ./cmd/server

docker-build:
	docker build -t ai-crypto-onramp/blockchain-gateway .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/blockchain-gateway

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

e2e-smoke:
	go test ./test/e2e/ -v -race -timeout 30s

clean:
	rm -rf bin/ coverage.out

migrate-up:
	go run ./cmd/migrate --up

migrate-down:
	go run ./cmd/migrate --down