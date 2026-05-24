#!/usr/bin/env bash
# =============================================================
# KalpanaOS Vault Initialisation Script
# Run this ONCE after `make up` to seed secrets into Vault.
# Usage: ./scripts/vault-init.sh
# =============================================================
set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-kalpana-vault-root-token}"

# Load .env if present
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

export VAULT_ADDR
export VAULT_TOKEN

echo "=== KalpanaOS Vault Initialisation ==="
echo "  Vault address: $VAULT_ADDR"

# Wait for Vault to be ready
echo -n "Waiting for Vault..."
for i in $(seq 1 30); do
  if curl -sf "$VAULT_ADDR/v1/sys/health" >/dev/null 2>&1; then
    echo " ready!"
    break
  fi
  echo -n "."
  sleep 2
done

# Enable KV v2 secrets engine
echo "→ Enabling KV v2 secrets engine..."
curl -sf -X POST "$VAULT_ADDR/v1/sys/mounts/secret" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"kv","options":{"version":"2"}}' || true

# Write KalpanaOS secrets
echo "→ Writing JWT secret..."
curl -sf -X POST "$VAULT_ADDR/v1/secret/data/kalpana/jwt" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"secret\":\"${JWT_SECRET:-change-me-in-production}\"}}"

echo "→ Writing NVIDIA API key..."
curl -sf -X POST "$VAULT_ADDR/v1/secret/data/kalpana/nvidia" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"api_key\":\"${NVIDIA_API_KEY:-}\"}}"

echo "→ Writing admin credentials..."
curl -sf -X POST "$VAULT_ADDR/v1/secret/data/kalpana/admin" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"data\":{\"email\":\"${ADMIN_EMAIL:-admin@kalpana.local}\",\"password\":\"${ADMIN_PASSWORD:-changeme123!}\"}}"

# Create a policy for KalpanaOS services
echo "→ Creating kalpana-services policy..."
curl -sf -X POST "$VAULT_ADDR/v1/sys/policies/acl/kalpana-services" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": "path \"secret/data/kalpana/*\" { capabilities = [\"read\"] }\npath \"secret/metadata/kalpana/*\" { capabilities = [\"read\",\"list\"] }"
  }'

# Enable AppRole auth
echo "→ Enabling AppRole auth..."
curl -sf -X POST "$VAULT_ADDR/v1/sys/auth/approle" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type":"approle"}' || true

# Create role for KalpanaOS services
echo "→ Creating kalpana-svc AppRole..."
curl -sf -X POST "$VAULT_ADDR/v1/auth/approle/role/kalpana-svc" \
  -H "X-Vault-Token: $VAULT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"policies":["kalpana-services"],"token_ttl":"1h","token_max_ttl":"24h"}'

# Get Role ID and Secret ID
ROLE_ID=$(curl -sf "$VAULT_ADDR/v1/auth/approle/role/kalpana-svc/role-id" \
  -H "X-Vault-Token: $VAULT_TOKEN" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['role_id'])")

SECRET_ID=$(curl -sf -X POST "$VAULT_ADDR/v1/auth/approle/role/kalpana-svc/secret-id" \
  -H "X-Vault-Token: $VAULT_TOKEN" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['secret_id'])")

echo ""
echo "=== ✓ Vault Initialised Successfully ==="
echo ""
echo "  AppRole credentials (add to .env for services):"
echo "  VAULT_ROLE_ID=${ROLE_ID}"
echo "  VAULT_SECRET_ID=${SECRET_ID}"
echo ""
echo "  Vault UI: http://localhost:8200"
echo "  Root token: $VAULT_TOKEN"
echo ""
echo "  Secrets stored at:"
echo "    secret/kalpana/jwt      → JWT_SECRET"
echo "    secret/kalpana/nvidia   → NVIDIA_API_KEY"
echo "    secret/kalpana/admin    → admin credentials"
