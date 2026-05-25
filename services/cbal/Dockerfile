# ─────────────────────────────────────────────
# Stage 1: Builder (Debian Bookworm to match glibc and dynamic links)
# ─────────────────────────────────────────────
FROM golang:1.22-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    wget \
    ca-certificates \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Download and install libduckdb (C/C++ shared library)
# Version v0.10.2 matches marcboeker/go-duckdb dependency versions
RUN wget -q https://github.com/duckdb/duckdb/releases/download/v0.10.2/libduckdb-linux-amd64.zip -O /tmp/libduckdb.zip && \
    unzip /tmp/libduckdb.zip -d /usr/local/lib && \
    mv /usr/local/lib/duckdb.h /usr/local/include/ && \
    mv /usr/local/lib/duckdb.hpp /usr/local/include/ && \
    rm /tmp/libduckdb.zip

# Cache dependencies
COPY go.mod ./
RUN go mod download || true

# Build
COPY . .
RUN go mod tidy

# Build with CGO enabled to link DuckDB dynamic libraries
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-w -s" \
    -o cbal \
    .

# ─────────────────────────────────────────────
# Stage 2: Final minimal image
# ─────────────────────────────────────────────
FROM debian:bookworm-slim

# Runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Setup non-root user
RUN groupadd -g 1000 cbal && useradd -u 1000 -g cbal -m -s /bin/bash cbal

# Setup directory for SQLite / DuckDB
RUN mkdir -p /data && chown cbal:cbal /data

WORKDIR /app

# Copy binaries and libduckdb
COPY --from=builder /build/cbal .
COPY --from=builder /usr/local/lib/libduckdb.so /usr/local/lib/

# Update linker runtime path cache
RUN ldconfig /usr/local/lib

# Ensure correct permissions
RUN chown cbal:cbal /app/cbal && chmod +x /app/cbal

USER cbal

EXPOSE 8010

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -qO- http://localhost:8010/health || exit 1

ENTRYPOINT ["/app/cbal"]
