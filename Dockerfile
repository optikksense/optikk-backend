FROM golang:1.24-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build -ldflags="-s -w" -o observability-go ./cmd/server

FROM alpine:3.20
WORKDIR /app

COPY --from=builder /app/observability-go .

RUN chown -R 1000:1000 /app
USER 1000:1000

EXPOSE 8080
ENTRYPOINT ["./observability-go"]