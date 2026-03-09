# Docker — Optikk Backend

Reference guide for building, tagging, and publishing the backend image to Docker Hub.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) ≥ 24
- A Docker Hub account with push access to the repository
- Logged in: `docker login`

---

## Image details

| Attribute | Value |
|---|---|
| Binary | `observability-go` |
| Exposed port | `8080` |
| Base (runtime) | `alpine:3.20` |
| Build base | `golang:1.24-alpine` |

---

## 1 · Build

Run from the repo root (where `Dockerfile` lives):

```bash
# Syntax: docker build -t <image>:<tag> .
docker build -t optikk-backend:latest .
```

Pass `--platform` if you need a specific architecture (e.g. for a Linux server from an Apple Silicon Mac):

```bash
docker build --platform linux/amd64 -t optikk-backend:latest .
```

### Build arguments / caching tips

The Dockerfile downloads Go modules before copying source, so the `go mod download` layer is cached as long as `go.mod` / `go.sum` haven't changed. Take advantage of this by not touching those files unnecessarily.

---

## 2 · Tag

Always tag with both a version and `latest`:

```bash
# Replace <tagname> as appropriate
docker tag optikk-backend:latest optikk/optic-iris:latest
docker tag optikk-backend:latest optikk/optic-iris:<tagname>

# Example
docker tag optikk-backend:latest optikk/optic-iris:latest
docker tag optikk-backend:latest optikk/optic-iris:1.0.0
```

---

## 3 · Push

```bash
docker push optikk/optic-iris:latest
docker push optikk/optic-iris:<tagname>

# Example
docker push optikk/optic-iris:latest
docker push optikk/optic-iris:1.0.0
```

---

## 4 · One-liner (build → tag → push)

```bash
VERSION=1.0.0
REPO=optikk/optic-iris

docker build --platform linux/amd64 -t $REPO:$VERSION -t $REPO:latest . \
  && docker push $REPO:$VERSION \
  && docker push $REPO:latest
```

---

## 5 · Multi-platform builds (BuildKit)

To ship a single manifest that supports both `linux/amd64` and `linux/arm64`:

```bash
# One-time setup — create a multi-platform builder
docker buildx create --use --name multi-builder

VERSION=1.0.0
REPO=optikk/optic-iris

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag $REPO:$VERSION \
  --tag $REPO:latest \
  --push \
  .
```

> **Note:** `--push` is required with `buildx` because multi-platform images can't be loaded into the local Docker daemon directly.

---

## 6 · Running the image locally

```bash
docker run --rm \
  -p 8080:8080 \
  -e DB_DSN="user:pass@tcp(host:3306)/dbname" \
  -e CLICKHOUSE_DSN="clickhouse://host:9000/dbname" \
  -e JWT_SECRET="your-secret" \
  optikk/optic-iris:latest
```

See `internal/config/config.go` for the full list of supported environment variables.

---

## 7 · Environment variables (required at runtime)

| Variable | Description |
|---|---|
| `DB_DSN` | MySQL connection string |
| `CLICKHOUSE_DSN` | ClickHouse connection string |
| `JWT_SECRET` | Secret used to sign JWTs |
| `JWT_EXPIRATION_MS` | Token TTL in milliseconds (default: `86400000`) |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret |
| `GITHUB_CLIENT_ID` | GitHub OAuth client ID |
| `GITHUB_CLIENT_SECRET` | GitHub OAuth client secret |
| `OAUTH_REDIRECT_BASE` | Base URL the OAuth provider redirects to (e.g. `https://api.optikk.io`) |

---

## 8 · CI / GitHub Actions snippet

```yaml
- name: Build and push backend image
  uses: docker/build-push-action@v5
  with:
    context: .
    platforms: linux/amd64,linux/arm64
    push: true
    tags: |
      optikk/optic-iris:${{ github.sha }}
      optikk/optic-iris:latest
```
