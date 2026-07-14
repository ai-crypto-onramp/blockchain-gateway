# syntax=docker/dockerfile:1.6

# --- builder ---
FROM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/blockchain-gateway .

# --- runtime ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/blockchain-gateway /blockchain-gateway
EXPOSE 8080 9090 9100
USER nonroot:nonroot
ENTRYPOINT ["/blockchain-gateway"]