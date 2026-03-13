# Build stage
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o data-agent main.go

# Runtime stage
FROM alpine:latest
WORKDIR /
COPY --from=builder /app/data-agent /data-agent

# Create data directory
RUN mkdir -p /data/dag-outputs

EXPOSE 8080
ENTRYPOINT ["/data-agent"]
