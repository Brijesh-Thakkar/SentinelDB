#!/bin/bash
# Test Policy Persistence Across Restarts

echo "========================================================================"
echo "  Policy Persistence Test"
echo "========================================================================"
echo ""

# Clean up old test data
rm -f data/wal.log data/snapshot.db

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
        ((TEST_FAILED++))
    fi
}

# TEST 1: Policy persisted via WAL
echo "Test 1: Policy persisted via WAL"

cat > /tmp/test1.txt << 'EOF'
POLICY SET STRICT
SET key1 value1
EXIT
EOF

./build/redis_db < /tmp/test1.txt 2>&1 > /dev/null

if [ -f data/wal.log ]; then
    WAL_CONTENT=$(cat data/wal.log)
    check_result "WAL contains POLICY SET STRICT" "POLICY SET STRICT" "$WAL_CONTENT"
fi

cat > /tmp/test2.txt << 'EOF'
POLICY GET
EXIT
EOF

OUTPUT=$(./build/redis_db < /tmp/test2.txt 2>&1)
check_result "Policy restored to STRICT" "STRICT" "$OUTPUT"

# TEST 2: Multiple policy changes - last wins
echo ""
echo "Test 2: Multiple policy changes"

cat > /tmp/test3.txt << 'EOF'
POLICY SET STRICT
POLICY SET DEV_FRIENDLY
POLICY SET SAFE_DEFAULT
EXIT
EOF

./build/redis_db < /tmp/test3.txt 2>&1 > /dev/null

OUTPUT=$(./build/redis_db < /tmp/test2.txt 2>&1)
check_result "Last policy (SAFE_DEFAULT) wins" "SAFE_DEFAULT" "$OUTPUT"

# TEST 3: Policy in snapshot
echo ""
echo "Test 3: Policy in snapshot"

rm -f data/wal.log data/snapshot.db

cat > /tmp/test4.txt << 'EOF'
POLICY SET DEV_FRIENDLY
SET snap1 value1
SNAPSHOT
EXIT
EOF

./build/redis_db < /tmp/test4.txt 2>&1 > /dev/null

if [ -f data/snapshot.db ]; then
    SNAPSHOT_CONTENT=$(cat data/snapshot.db)
    check_result "Snapshot contains policy" "POLICY SET DEV_FRIENDLY" "$SNAPSHOT_CONTENT"
fi

rm -f data/wal.log

OUTPUT=$(./build/redis_db < /tmp/test2.txt 2>&1)
check_result "Policy restored from snapshot" "DEV_FRIENDLY" "$OUTPUT"

# TEST 4: Default policy
echo ""
echo "Test 4: Default policy"

rm -f data/wal.log data/snapshot.db

OUTPUT=$(./build/redis_db < /tmp/test2.txt 2>&1)
check_result "Default is SAFE_DEFAULT" "SAFE_DEFAULT" "$OUTPUT"

# Summary
echo ""
echo "========================================================================"
echo "Passed: $TEST_PASSED / Failed: $TEST_FAILED"

rm -f /tmp/test*.txt

if [ $TEST_FAILED -eq 0 ]; then
    echo "✓ All tests passed!"
    exit 0
else
    exit 1
fi
