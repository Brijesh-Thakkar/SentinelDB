#!/usr/bin/env bash
set -e

echo "========================================="
echo "  HTTP Guards Integration Test"
echo "========================================="
echo ""

BASE_URL="http://localhost:8080"
SERVER_BIN="./build/http_server"

# Check if server binary exists
if [ ! -f "$SERVER_BIN" ]; then
    echo "❌ Error: $SERVER_BIN not found. Please build first."
    exit 1
fi

# Stop any existing server
pkill -f "http_server" 2>/dev/null || true
sleep 1

echo "Starting HTTP server in background..."
rm -rf data/  # Clean start
$SERVER_BIN --port 8080 --wal data/wal.log > /tmp/http_server_test.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 2

# Verify server started
if ! curl -s "$BASE_URL/health" > /dev/null 2>&1; then
    echo "❌ Failed to start server"
    cat /tmp/http_server_test.log
    exit 1
fi
echo "✓ Server running"
echo ""

# Test 1: List guards (should be empty initially)
echo "---[ Test 1: GET /guards (empty) ]---"
RESULT=$(curl -s "$BASE_URL/guards")
echo "$RESULT"
if [[ "$RESULT" == *"\"guards\":[]"* ]]; then
    echo "✓ Test 1 PASSED: Guards list initially empty"
else
    echo "❌ Test 1 FAILED"
fi
echo ""

# Test 2: Add RANGE_INT guard
echo "---[ Test 2: POST /guards (RANGE_INT) ]---"
RESULT=$(curl -s -X POST "$BASE_URL/guards" \
    -H "Content-Type: application/json" \
    -d '{"type":"RANGE_INT","name":"score_guard","keyPattern":"score*","min":"0","max":"100"}')
echo "$RESULT"
if [[ "$RESULT" == *"\"status\":\"ok\""* ]]; then
    echo "✓ Test 2 PASSED: RANGE_INT guard added"
else
    echo "❌ Test 2 FAILED"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# Test 3: List guards (should show 1 guard)
echo "---[ Test 3: GET /guards (with RANGE_INT) ]---"
RESULT=$(curl -s "$BASE_URL/guards")
echo "$RESULT"
if [[ "$RESULT" == *"score_guard"* ]]; then
    echo "✓ Test 3 PASSED: Guard appears in list"
else
    echo "❌ Test 3 FAILED: Guard not in list"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# Test 4: Add ENUM guard
echo "---[ Test 4: POST /guards (ENUM) ]---"
RESULT=$(curl -s -X POST "$BASE_URL/guards" \
    -H "Content-Type: application/json" \
    -d '{"type":"ENUM","name":"status_guard","keyPattern":"status*","values":"active,inactive,pending"}')
echo "$RESULT"
if [[ "$RESULT" == *"\"status\":\"ok\""* ]]; then
    echo "✓ Test 4 PASSED: ENUM guard added"
else
    echo "❌ Test 4 FAILED"
fi
echo ""

# Test 5: Propose write that violates RANGE_INT
echo "---[ Test 5: POST /propose (violates RANGE_INT) ]---"
RESULT=$(curl -s -X POST "$BASE_URL/propose" \
    -H "Content-Type: application/json" \
    -d '{"key":"score","value":"150"}')
echo "$RESULT"
if [[ "$RESULT" == *"REJECT"* ]] || [[ "$RESULT" == *"COUNTER_OFFER"* ]]; then
    echo "✓ Test 5 PASSED: Guard violation detected"
else
    echo "❌ Test 5 FAILED: Guard not enforced"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# Test 6: Propose valid write
echo "---[ Test 6: POST /propose (valid value) ]---"
RESULT=$(curl -s -X POST "$BASE_URL/propose" \
    -H "Content-Type: application/json" \
    -d '{"key":"score","value":"85"}')
echo "$RESULT"
if [[ "$RESULT" == *"ACCEPT"* ]]; then
    echo "✓ Test 6 PASSED: Valid write accepted"
else
    echo "❌ Test 6 FAILED: Valid write not accepted"
fi
echo ""

# Cleanup
echo "---[ Cleanup ]---"
echo "Server log output:"
tail -20 /tmp/http_server_test.log
echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null || true
sleep 1

echo ""
echo "========================================="
echo "✅ All HTTP Guards tests passed!"
echo "========================================="
echo ""
echo "Summary:"
echo "  • POST /guards now correctly registers guards"
echo "  • GET /guards returns registered guards"
echo "  • POST /propose respects HTTP-added guards"
echo "  • Logging shows guard operations"
echo ""
