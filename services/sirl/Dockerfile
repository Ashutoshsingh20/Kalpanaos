# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

ENV CGO_ENABLED=1

RUN apk add --no-cache \
    gcc \
    musl-dev \
    sqlite-dev

WORKDIR /build

COPY go.mod ./
RUN go mod download || true

COPY . .
RUN go mod tidy

RUN go build \
    -tags "sqlite_foreign_keys" \
    -ldflags "-s -w -linkmode external -extldflags '-static'" \
    -o sirl \
    .

# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache \
    ca-certificates \
    sqlite-libs \
    wget \
    git

RUN addgroup -g 1000 kalpana && \
    adduser  -u 1000 -G kalpana -s /bin/sh -D kalpana

RUN mkdir -p /data && chown kalpana:kalpana /data

WORKDIR /app

COPY --from=builder /build/sirl .
RUN chown kalpana:kalpana /app/sirl && chmod 550 /app/sirl

# Running under root is needed to access host /var/run/docker.sock
USER root

EXPOSE 8011

ENV PORT=8011 \
    DB_PATH=/data/sirl.db \
    SIL_URL=http://sil:8001 \
    PGE_URL=http://pge:8007 \
    IKG_URL=http://ikg:8008 \
    SSI_URL=http://ssi:8003 \
    CBAL_URL=http://cbal:8010 \
    NATS_URL=nats://nats:4222 \
    DOCKER_NETWORK=kalpana-net \
    DOCKER_HOST=unix:///var/run/docker.sock

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- --ca-certificate=/certs/ca.crt --certificate=/certs/sirl/client.crt --private-key=/certs/sirl/client.key https://localhost:${PORT}/health || exit 1

ENTRYPOINT ["/app/sirl"]
