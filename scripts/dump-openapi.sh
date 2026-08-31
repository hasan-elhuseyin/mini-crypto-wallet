#!/usr/bin/env bash
# Save both OpenAPI documents to docs/openapi/ (the stack must be running).
set -euo pipefail
mkdir -p docs/openapi
curl -fsS "${WALLET_URL:-http://localhost:8000}/openapi.json" \
  | python3 -m json.tool > docs/openapi/wallet-service.json
curl -fsS "${CHAIN_URL:-http://localhost:8001}/openapi.json" \
  | python3 -m json.tool > docs/openapi/blockchain-service.json
echo "wrote docs/openapi/{wallet-service,blockchain-service}.json"
