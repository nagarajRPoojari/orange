# syntax=docker/dockerfile:1

# Use the latest Go toolchain
FROM golang:1.24.5-bookworm AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -o proxy main.go

# Final runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/proxy .
RUN mkdir -p /app/config

EXPOSE 8000
CMD ["./proxy"]
