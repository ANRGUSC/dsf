# ---- Build stage ----
FROM golang:1.24.5 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o random-controller main.go

# ---- Runtime stage ----
FROM alpine:latest
WORKDIR /root
COPY --from=builder /app/random-controller .
ENTRYPOINT ["./random-controller"]


