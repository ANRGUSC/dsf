FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /cdag-controller ./cmd/cdag-controller

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /cdag-controller /cdag-controller
ENTRYPOINT ["/cdag-controller"]
