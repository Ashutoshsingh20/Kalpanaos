# ─────────────────────────────────────────────
# Stage 1: Build
# ─────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

# Install build dependencies (CGO disabled so no gcc needed,
# but we keep git for potential go mod fetch over VCS)
RUN apk add --no-cache git

WORKDIR /build

# Cache dependency downloads separately from source build
COPY go.mod go.sum* ./
RUN go mod download || true

# Copy source and build a fully-static binary
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /aicp \
    .

# ─────────────────────────────────────────────
# Stage 2: Runtime
# ─────────────────────────────────────────────
FROM alpine:3.19

# Security updates + CA bundle for HTTPS calls to NVIDIA API
RUN apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates

# Non-root user
RUN addgroup -S aicp && adduser -S -G aicp aicp

# Persistent state directory
RUN mkdir -p /data && chown -R aicp:aicp /data
VOLUME ["/data"]

# Copy binary from builder
COPY --from=builder /aicp /usr/local/bin/aicp
RUN chmod 755 /usr/local/bin/aicp

USER aicp

EXPOSE 8004

# Health check using the /health endpoint
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8004/health || exit 1

ENTRYPOINT ["/usr/local/bin/aicp"]
