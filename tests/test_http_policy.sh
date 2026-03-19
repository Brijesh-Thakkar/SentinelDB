#!/bin/bash
# Test HTTP Policy Endpoints

echo "========================================================================"
echo "  HTTP Policy Endpoints Test"
echo "========================================================================"
echo ""

# Start HTTP server in background
echo "Starting HTTP server..."
rm -f data/wal.log data/snapshot.db
./build/http_server --port 8888 --wal data/wal.log > /dev/null 2>&1 &
HTTP_PID=$!

# Wait for server to start
sleep 2

if ! kill -0 $HTTP_PID 2>/dev/null; then
    echo "✗ Failed to start HTTP server"
    exit 1
fi

echo "Server started (PID: $HTTP_PID)"
echo ""

TEST_PASSED=0
TEST_FAILED=0

check_result() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    
    if echo "$actual" | grep -q "$expected"; then
        echo "✓ $test_name"
        ((TEST_PASSED++))
    else
        echo "✗ $test_name"
        echo "  Expected: $expected"
        echo "  Got: $actual"
        ((TEST_FAILED++))
    fi
}

# TEST 1: GET /policy (default)
echo "Test 1: GET /policy - default policy"
RESPONSE=$(curl -s http://localhost:8888/policy)
check_result "Default policy is SAFE_DEFAULT" "SAFE_DEFAULT" "$RESPONSE"
check_result "Contains description" "description" "$RESPONSE"

# TEST 2: POST /policy
echo ""
echo "Test 2: POST /policy - set to STRICT"
RESPONSE=$(curl -s -X POST http://localhost:8888/policy \
    -H "Content-Type: application/json" \
    -d '{"policy":"STRICT"}')
check_result "POST returns ok" "\"status\":\"ok\"" "$RESPONSE"
check_result "POST confirms STRICT" "\"activePolicy\":\"STRICT\"" "$RESPONSE"

# TEST 3: GET /policy after POST
echo ""
echo "Test 3: GET /policy - verify STRICT"
RESPONSE=$(curl -s http://localhost:8888/policy)
check_result "Policy changed to STRICT" "STRICT" "$RESPONSE"

# TEST 4: Invalid policy
echo ""
echo "Test 4: POST invalid policy"
RESPONSE=$(curl -s -X POST http://localhost:8888/policy \
    -H "Content-Type: application/json" \
    -d '{"policy":"INVALID"}')
check_result "Returns error for invalid policy" "error" "$RESPONSE"

# TEST 5: Missing policy parameter
echo ""
echo "Test 5: POST without policy parameter"
RESPONSE=$(curl -s -X POST http://localhost:8888/policy \
    -H "Content-Type: application/json" \
    -d '{}')
check_result "Returns error for missing parameter" "Missing" "$RESPONSE"

# TEST 6: Policy affects /propose
echo ""
echo "Test 6: Policy affects /propose behavior"

# First set DEV_FRIENDLY
curl -s -X POST http://localhost:8888/policy \
    -H "Content-Type: application/json" \
    -d '{"policy":"DEV_FRIENDLY"}' > /dev/null

# Add a guard (note: guards are not persisted, so this is in-memory only)
# For now, just verify policy is applied
RESPONSE=$(curl -s http://localhost:8888/policy)
check_result "Policy set to DEV_FRIENDLY" "DEV_FRIENDLY" "$RESPONSE"

# TEST 7: Policy persisted in WAL via HTTP
echo ""
echo "Test 7: HTTP policy changes persisted in WAL"

# Kill server
kill $HTTP_PID 2>/dev/null
sleep 1

# Check WAL
if [ -f data/wal.log ]; then
    WAL_CONTENT=$(cat data/wal.log)
    check_result "WAL contains POLICY commands" "POLICY SET" "$WAL_CONTENT"
fi

# Restart server
./build/http_server --port 8888 --wal data/wal.log > /dev/null 2>&1 &
HTTP_PID=$!
sleep 2

# Check policy is restored
RESPONSE=$(curl -s http://localhost:8888/policy)
check_result "Policy restored after restart" "DEV_FRIENDLY" "$RESPONSE"

# Cleanup
kill $HTTP_PID 2>/dev/null
wait $HTTP_PID 2>/dev/null

echo ""
echo "========================================================================"
echo "Passed: $TEST_PASSED / Failed: $TEST_FAILED"
echo ""

if [ $TEST_FAILED -eq 0 ]; then
    echo "✓ All HTTP tests passed!"
    exit 0
else
    echo "✗ Some tests failed"
    exit 1
fi
