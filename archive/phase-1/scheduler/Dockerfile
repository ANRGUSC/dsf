# Build stage
FROM golang:1.21-alpine AS builder

# Install git for go mod download
RUN apk add --no-cache git

WORKDIR /app

# Copy go mod files first for caching
COPY go.mod ./
RUN go mod download

# Copy source code
COPY . .

# Build the scheduler
RUN CGO_ENABLED=0 GOOS=linux go build -o dag-scheduler ./cmd/dag-scheduler

# Runtime stage
FROM alpine:latest

WORKDIR /

COPY --from=builder /app/dag-scheduler /usr/local/bin/dag-scheduler

ENTRYPOINT ["/usr/local/bin/dag-scheduler"]
