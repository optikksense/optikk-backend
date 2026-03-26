FROM golang:1.25-alpine AS builder
WORKDIR /app

# Copy workspace and all module manifests first for layer caching
COPY go.work go.work.sum ./
COPY go.mod go.sum ./
COPY cmd/server/go.mod cmd/server/go.sum ./cmd/server/
COPY internal/platform/server/go.mod internal/platform/server/go.sum ./internal/platform/server/

RUN go work sync && go mod download

COPY . .
RUN go work use && \
    CGO_ENABLED=0 GOOS=linux \
    go build -ldflags="-s -w" -o observability-go ./cmd/server

FROM alpine:3.20
WORKDIR /app

COPY --from=builder /app/observability-go .
COPY config.yml .
COPY migrations/ ./migrations/

RUN chown -R 1000:1000 /app
USER 1000:1000

EXPOSE 8080
ENTRYPOINT ["./observability-go"]