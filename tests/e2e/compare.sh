#!/bin/bash
# Compare two chaintracks instances endpoint-by-endpoint.
# Usage: ./tests/e2e/compare.sh <under_test_url> <reference_url>
#
# Verifies 1:1 parity between an instance under test (typically this worker)
# and a reference chaintracks instance known to be correct.

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <under_test_url> <reference_url>" >&2
    exit 2
fi
RUST_URL="$1"
PROD_URL="$2"

PASS=0
FAIL=0
SKIP=0

compare() {
    local endpoint="$1"
    local description="$2"
    local compare_mode="${3:-exact}"  # exact, json_value, status_only

    local rust_resp prod_resp

    rust_resp=$(curl -sf "$RUST_URL$endpoint" 2>/dev/null)
    local rust_code=$?
    prod_resp=$(curl -sf "$PROD_URL$endpoint" 2>/dev/null)
    local prod_code=$?

    # Skip if production returns error (some endpoints don't exist on old server)
    if [ $prod_code -ne 0 ]; then
        echo "SKIP $endpoint — production unavailable"
        SKIP=$((SKIP + 1))
        return
    fi

    if [ $rust_code -ne 0 ]; then
        echo "FAIL $endpoint — rust returned error"
        echo "  PROD: $prod_resp"
        FAIL=$((FAIL + 1))
        return
    fi

    case "$compare_mode" in
        exact)
            if [ "$rust_resp" = "$prod_resp" ]; then
                echo "PASS $endpoint — $description"
                PASS=$((PASS + 1))
            else
                echo "FAIL $endpoint — $description"
                echo "  RUST: $rust_resp"
                echo "  PROD: $prod_resp"
                FAIL=$((FAIL + 1))
            fi
            ;;
        json_value)
            # Compare the "value" field — normalize key order with sort_keys
            local rust_val prod_val
            rust_val=$(echo "$rust_resp" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin).get('value',''), sort_keys=True))" 2>/dev/null)
            prod_val=$(echo "$prod_resp" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin).get('value',''), sort_keys=True))" 2>/dev/null)
            if [ "$rust_val" = "$prod_val" ]; then
                echo "PASS $endpoint — $description (value match)"
                PASS=$((PASS + 1))
            else
                echo "FAIL $endpoint — $description"
                echo "  RUST value: $rust_val"
                echo "  PROD value: $prod_val"
                FAIL=$((FAIL + 1))
            fi
            ;;
        status_only)
            # Just check both return {status: "success"}
            local rust_status prod_status
            rust_status=$(echo "$rust_resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
            prod_status=$(echo "$prod_resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
            if [ "$rust_status" = "success" ] && [ "$prod_status" = "success" ]; then
                echo "PASS $endpoint — $description (both success)"
                PASS=$((PASS + 1))
            else
                echo "FAIL $endpoint — $description"
                echo "  RUST status: $rust_status"
                echo "  PROD status: $prod_status"
                FAIL=$((FAIL + 1))
            fi
            ;;
    esac
}

echo "═══════════════════════════════════════════════════════════"
echo "Comparing: $RUST_URL vs $PROD_URL"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ─── Health ──────────────────────────────────────────────────
compare "/" "Root health check" exact

# ─── Chain info ──────────────────────────────────────────────
compare "/getChain" "Chain identifier" json_value

# ─── getInfo ─────────────────────────────────────────────────
compare "/getInfo" "Service info" status_only

# ─── Chain tip (only compare if rust has data) ───────────────
compare "/findChainTipHashHex" "Chain tip hash" status_only

# ─── Known block vectors ─────────────────────────────────────
# These test against height 0 (genesis) which should match exactly
# once the instance has data seeded
compare "/findHeaderHexForHeight?height=0" "Genesis header" json_value
compare "/getHeaders?height=0&count=1" "Genesis header hex" json_value
compare "/getHeaders?height=0&count=2" "Genesis+Block1 hex" json_value

# ─── Random height sampling (only works with seeded data) ────
for height in 100 1000 10000 100000 500000 800000; do
    compare "/findHeaderHexForHeight?height=$height" "Header at height $height" json_value
done

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "═══════════════════════════════════════════════════════════"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
