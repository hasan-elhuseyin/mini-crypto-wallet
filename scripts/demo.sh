#!/usr/bin/env bash
# End-to-end demonstration of the case scenario.
#
#   1. create User A and User B
#   2. give each a blockchain wallet
#   3. simulate a 1.000 USDT deposit to User A
#   4. wait for it to reach the confirmation threshold
#   5. transfer 250 USDT from A to B -- sent three times with the same
#      idempotency key, to prove only one transfer is created
#   6. wait for on-chain settlement
#   7. assert the final balances are A = 750, B = 250
#
# Requires the stack to be running: `make up`.
set -euo pipefail

WALLET_URL="${WALLET_URL:-http://localhost:8000}"
CHAIN_URL="${CHAIN_URL:-http://localhost:8001}"
API_KEY="${API_KEY:-dev-api-key-change-me}"
INTERNAL_API_KEY="${INTERNAL_API_KEY:-dev-internal-key-change-me}"
CORRELATION_ID="demo-$(date +%s)"
SUFFIX="$(date +%s)"

if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  set -a; source .env; set +a
fi

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
fail() { printf '\033[31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

jsonget() { python3 -c 'import json,sys;d=json.load(sys.stdin);ks=sys.argv[1].split(".");
for k in ks: d=d[int(k)] if k.isdigit() else d[k]
print(d)' "$1"; }

wallet_api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$WALLET_URL$path" \
      -H "X-API-Key: $API_KEY" -H "X-Correlation-ID: $CORRELATION_ID" \
      -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X "$method" "$WALLET_URL$path" \
      -H "X-API-Key: $API_KEY" -H "X-Correlation-ID: $CORRELATION_ID"
  fi
}

chain_api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$CHAIN_URL$path" \
      -H "X-Internal-Key: $INTERNAL_API_KEY" -H "X-Correlation-ID: $CORRELATION_ID" \
      -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X "$method" "$CHAIN_URL$path" -H "X-Internal-Key: $INTERNAL_API_KEY"
  fi
}

wait_for() {
  local name="$1" url="$2" tries=60
  printf '  waiting for %s' "$name"
  until curl -fsS "$url" >/dev/null 2>&1; do
    ((tries--)) || { echo; fail "$name never became healthy"; }
    printf '.'; sleep 1
  done
  echo ' ok'
}

bold "0. Waiting for services"
wait_for "wallet-service"     "$WALLET_URL/health/live"
wait_for "blockchain-service" "$CHAIN_URL/health/live"
info "correlation id for this run: $CORRELATION_ID"

bold "1. Creating users"
USER_A=$(wallet_api POST /users "{\"name\":\"User A\",\"email\":\"user-a+$SUFFIX@example.com\"}" | jsonget id)
USER_B=$(wallet_api POST /users "{\"name\":\"User B\",\"email\":\"user-b+$SUFFIX@example.com\"}" | jsonget id)
info "User A id=$USER_A"
info "User B id=$USER_B"

bold "2. Creating wallets"
ADDR_A=$(wallet_api POST "/users/$USER_A/wallet" '{}' | jsonget address)
ADDR_B=$(wallet_api POST "/users/$USER_B/wallet" '{}' | jsonget address)
info "User A address: $ADDR_A"
info "User B address: $ADDR_B"
[[ "$ADDR_A" != "$ADDR_B" ]] || fail "both users got the same address"

bold "3. Simulating a 1000 USDT deposit to User A"
TX_HASH=$(chain_api POST /simulate/deposits \
  "{\"to_address\":\"$ADDR_A\",\"amount\":\"1000.000000\",\"asset\":\"USDT\"}" | jsonget tx_hash)
info "deposit tx: $TX_HASH"

bold "4. Waiting for the deposit to confirm"
for _ in $(seq 1 60); do
  BALANCE_A=$(wallet_api GET "/users/$USER_A/balance" | jsonget available)
  [[ "$BALANCE_A" == "1000.000000" ]] && break
  sleep 1
done
[[ "$BALANCE_A" == "1000.000000" ]] || fail "deposit never credited (balance=$BALANCE_A)"
info "User A available: $BALANCE_A USDT"

bold "5. Transferring 250 USDT (same idempotency key, sent 3 times)"
BODY="{\"from_user_id\":$USER_A,\"to_user_id\":$USER_B,\"asset\":\"USDT\",\"amount\":\"250.000000\",\"idempotency_key\":\"transfer-001-$SUFFIX\"}"
ID1=$(wallet_api POST /transfers "$BODY" | jsonget id)
ID2=$(wallet_api POST /transfers "$BODY" | jsonget id)
ID3=$(wallet_api POST /transfers "$BODY" | jsonget id)
info "attempt 1 -> $ID1"
info "attempt 2 -> $ID2"
info "attempt 3 -> $ID3"
[[ "$ID1" == "$ID2" && "$ID2" == "$ID3" ]] || fail "idempotency key produced more than one transfer"

bold "6. Waiting for on-chain settlement"
for _ in $(seq 1 90); do
  STATUS=$(wallet_api GET "/transfers/$ID1" | jsonget status)
  [[ "$STATUS" == "CONFIRMED" || "$STATUS" == "FAILED" ]] && break
  sleep 1
done
info "transfer status: $STATUS"
[[ "$STATUS" == "CONFIRMED" ]] || fail "transfer ended as $STATUS"
info "tx hash: $(wallet_api GET "/transfers/$ID1" | jsonget tx_hash)"

bold "7. Final balances"
FINAL_A=$(wallet_api GET "/users/$USER_A/balance" | jsonget available)
FINAL_B=$(wallet_api GET "/users/$USER_B/balance" | jsonget available)
info "User A: $FINAL_A USDT"
info "User B: $FINAL_B USDT"
[[ "$FINAL_A" == "750.000000" ]] || fail "expected User A = 750.000000, got $FINAL_A"
[[ "$FINAL_B" == "250.000000" ]] || fail "expected User B = 250.000000, got $FINAL_B"

bold "8. Ledger history for User A"
wallet_api GET "/users/$USER_A/transactions" | python3 -m json.tool

bold "9. Reconciliation (snapshots vs. ledger)"
wallet_api GET /admin/reconciliation | python3 -m json.tool

printf '\n\033[32mAll checks passed: A = %s, B = %s\033[0m\n' "$FINAL_A" "$FINAL_B"
printf 'Trace this run across both services with: correlation_id=%s\n' "$CORRELATION_ID"
