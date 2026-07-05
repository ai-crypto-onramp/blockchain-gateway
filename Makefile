.PHONY: build test run lint docker-build docker-run clean

build:
	go build -o bin/server .

test:
	go test ./... -race -coverprofile=coverage.out -coverpkg=./...

run:
	go run .

lint:
	go vet ./...

docker-build:
	docker build -t ai-crypto-onramp/blockchain-gateway .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/blockchain-gateway

clean:
	rm -rf bin/ coverage.out
