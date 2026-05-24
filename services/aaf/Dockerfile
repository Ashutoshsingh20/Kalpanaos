# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

# Install build dependencies for CGO + SQLite
RUN apk add --no-cache \
        gcc \
        musl-dev \
        sqlite-dev \
        git

WORKDIR /build

# Cache dependency downloads before copying source
COPY go.mod go.sum* ./
RUN go mod download || true

# Copy source
COPY . .
RUN go mod tidy

# Build with CGO enabled for go-sqlite3
RUN CGO_ENABLED=1 GOOS=linux go build \
        -ldflags="-s -w -extldflags '-static'" \
        -tags "sqlite_foreign_keys netgo" \
        -o /aaf-server \
        .

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM alpine:3.19

# Install runtime SQLite libraries and CA certs
RUN apk add --no-cache \
        sqlite-libs \
        ca-certificates \
        tzdata

# Create non-root user (uid 1000)
RUN addgroup -g 1000 kalpana && \
    adduser  -u 1000 -G kalpana -s /sbin/nologin -D kalpana

# Create data directory and set ownership
RUN mkdir -p /data && chown kalpana:kalpana /data

# Copy compiled binary
COPY --from=builder /aaf-server /usr/local/bin/aaf-server
RUN chmod +x /usr/local/bin/aaf-server

# Switch to non-root
USER kalpana

# Persistent SQLite data volume
VOLUME ["/data"]

# Service port
EXPOSE 8005

# Default environment
ENV PORT=8005 \
    DB_PATH=/data/aaf.db \
    SIL_URL=http://sil:8001 \
    SSI_URL=http://ssi:8002 \
    COL_URL=http://col:8003 \
    AICP_URL=http://aicp:8004 \
    NATS_URL=nats://nats:4222

ENTRYPOINT ["/usr/local/bin/aaf-server"]
