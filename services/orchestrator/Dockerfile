FROM golang:1.22-alpine AS builder
WORKDIR /build
RUN apk add --no-cache git
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o orchestrator .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates wget
RUN adduser -D -u 1000 kalpana
USER kalpana
WORKDIR /app
COPY --from=builder /build/orchestrator .
EXPOSE 8006
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
  CMD wget -qO- http://localhost:8006/health || exit 1
CMD ["./orchestrator"]
