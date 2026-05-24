#!/usr/bin/env bash
# =============================================================
# KalpanaOS Phase 3 — mTLS Certificate Generator
# Generates a self-signed CA + per-service TLS certificates
# Usage: bash scripts/gen-certs.sh
# =============================================================

set -euo pipefail

CERTS_DIR="deploy/certs"
SERVICES=("sil" "col" "ssi" "aicp" "aaf" "orchestrator")
DAYS_CA=3650
DAYS_SVC=365

echo "==================================================="
echo " KalpanaOS mTLS Certificate Generator"
echo "==================================================="

# Create directories
mkdir -p "$CERTS_DIR"
for svc in "${SERVICES[@]}"; do
  mkdir -p "$CERTS_DIR/$svc"
done

# ── Generate CA ──────────────────────────────────────────────
if [[ -f "$CERTS_DIR/ca.crt" ]]; then
  echo "[CA] ca.crt already exists — checking validity..."
  if openssl x509 -checkend 86400 -noout -in "$CERTS_DIR/ca.crt" 2>/dev/null; then
    echo "[CA] CA cert is still valid — skipping CA generation"
    SKIP_CA=1
  else
    echo "[CA] CA cert is expired — regenerating"
    SKIP_CA=0
  fi
else
  SKIP_CA=0
fi

if [[ $SKIP_CA -eq 0 ]]; then
  echo "[CA] Generating 4096-bit CA key..."
  openssl genrsa -out "$CERTS_DIR/ca.key" 4096 2>/dev/null

  echo "[CA] Generating CA certificate (valid ${DAYS_CA} days)..."
  openssl req -new -x509 \
    -key "$CERTS_DIR/ca.key" \
    -out "$CERTS_DIR/ca.crt" \
    -days $DAYS_CA \
    -subj "/CN=KalpanaOS-CA/O=KalpanaOS/C=IN" \
    2>/dev/null

  chmod 600 "$CERTS_DIR/ca.key"
  chmod 644 "$CERTS_DIR/ca.crt"
  echo "[CA] ✓ CA certificate generated"
fi

# ── Generate Service Certificates ─────────────────────────────
for svc in "${SERVICES[@]}"; do
  SVC_DIR="$CERTS_DIR/$svc"

  if [[ -f "$SVC_DIR/server.crt" ]]; then
    if openssl x509 -checkend 86400 -noout -in "$SVC_DIR/server.crt" 2>/dev/null; then
      echo "[$svc] certs already valid — skipping"
      continue
    fi
    echo "[$svc] cert expired — regenerating"
  fi

  echo "[$svc] Generating 2048-bit service key..."
  openssl genrsa -out "$SVC_DIR/server.key" 2048 2>/dev/null
  openssl genrsa -out "$SVC_DIR/client.key" 2048 2>/dev/null

  # Create SAN extension file
  cat > "/tmp/kalpana-san-$svc.ext" <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = $svc
DNS.2 = kalpana-$svc
DNS.3 = localhost
IP.1 = 127.0.0.1
EOF

  # Server cert
  echo "[$svc] Generating server CSR..."
  openssl req -new \
    -key "$SVC_DIR/server.key" \
    -out "/tmp/kalpana-server-$svc.csr" \
    -subj "/CN=$svc/O=KalpanaOS/C=IN" \
    2>/dev/null

  echo "[$svc] Signing server cert with CA..."
  openssl x509 -req \
    -in "/tmp/kalpana-server-$svc.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$SVC_DIR/server.crt" \
    -days $DAYS_SVC \
    -extfile "/tmp/kalpana-san-$svc.ext" \
    -extensions v3_req \
    2>/dev/null

  # Client cert (used for outbound mTLS calls)
  echo "[$svc] Generating client CSR..."
  openssl req -new \
    -key "$SVC_DIR/client.key" \
    -out "/tmp/kalpana-client-$svc.csr" \
    -subj "/CN=$svc-client/O=KalpanaOS/C=IN" \
    2>/dev/null

  echo "[$svc] Signing client cert with CA..."
  openssl x509 -req \
    -in "/tmp/kalpana-client-$svc.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$SVC_DIR/client.crt" \
    -days $DAYS_SVC \
    2>/dev/null

  # Set permissions
  chmod 600 "$SVC_DIR/server.key" "$SVC_DIR/client.key"
  chmod 644 "$SVC_DIR/server.crt" "$SVC_DIR/client.crt"

  # Cleanup temp files
  rm -f "/tmp/kalpana-san-$svc.ext" \
        "/tmp/kalpana-server-$svc.csr" \
        "/tmp/kalpana-client-$svc.csr"

  echo "[$svc] ✓ certs generated"
done

echo ""
echo "==================================================="
echo " Certificate Summary"
echo "==================================================="
echo ""
echo "CA Certificate:    $CERTS_DIR/ca.crt"
echo "CA Private Key:    $CERTS_DIR/ca.key  (keep secret!)"
echo ""
for svc in "${SERVICES[@]}"; do
  echo "Service: $svc"
  echo "  Server: $CERTS_DIR/$svc/server.crt + server.key"
  echo "  Client: $CERTS_DIR/$svc/client.crt + client.key"
done
echo ""
echo "Expiry:"
for svc in "${SERVICES[@]}"; do
  expiry=$(openssl x509 -enddate -noout -in "$CERTS_DIR/$svc/server.crt" 2>/dev/null | cut -d= -f2)
  echo "  $svc: $expiry"
done
echo ""
echo "==================================================="
echo " mTLS is ready. Restart the stack to activate:"
echo " docker compose down && docker compose up -d"
echo "==================================================="
