# Build stage
FROM golang:1.24 as build_container
WORKDIR /app

# Kopiraj go.mod i go.sum i preuzmi zavisnosti
COPY go.mod .
COPY go.sum .
RUN go mod download

# Kopiraj ostatak koda i builduj
COPY . .
RUN go build -o agent_queue ./cmd/server/main.go

# Final stage
FROM debian:bookworm-slim

# Instaliraj ca-certificates (poželjno za HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Kopiraj binarni fajl
COPY --from=build_container /app/agent_queue /usr/bin/

# Expose gRPC port
ARG GRPC_PORT
EXPOSE $GRPC_PORT

ENTRYPOINT ["agent_queue"]
