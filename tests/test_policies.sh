#!/bin/bash
# Automated test for decision policies
# This script verifies that all three policies behave correctly

echo "========================================"
echo "  Decision Policy Automated Test Suite"
echo "========================================"
echo ""

TEST_PASSED=0
TEST_FAILED=0

# Helper function to check output
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

# Test 1: STRICT rejects violations without alternatives
echo "Test 1: STRICT policy behavior"
OUTPUT=$(cat << 'EOF' | ./build/redis_db 2>&1
GUARD ADD RANGE_INT test_guard test* 0 100
POLICY SET STRICT
PROPOSE SET test 150
EXIT
EOF
)
check_result "STRICT rejects violation" "REJECT ✗" "$OUTPUT"
check_result "STRICT shows no alternatives" "This write cannot be performed" "$OUTPUT"

# Test 2: SAFE_DEFAULT negotiates when alternatives exist
echo ""
echo "Test 2: SAFE_DEFAULT negotiates with alternatives"
OUTPUT=$(cat << 'EOF' | ./build/redis_db 2>&1
GUARD ADD ENUM test_guard test* val1,val2,val3
POLICY SET SAFE_DEFAULT
PROPOSE SET test invalid
EXIT
EOF
)
check_result "SAFE_DEFAULT shows counter-offer" "COUNTER_OFFER ⚠" "$OUTPUT"
check_result "SAFE_DEFAULT shows alternatives" "Safe Alternatives:" "$OUTPUT"

# Test 3: DEV_FRIENDLY always negotiates
echo ""
echo "Test 3: DEV_FRIENDLY maximum negotiation"
OUTPUT=$(cat << 'EOF' | ./build/redis_db 2>&1
GUARD ADD RANGE_INT test_guard test* 0 100
POLICY SET DEV_FRIENDLY
PROPOSE SET test 200
EXIT
EOF
)
check_result "DEV_FRIENDLY shows counter-offer" "COUNTER_OFFER ⚠" "$OUTPUT"
check_result "DEV_FRIENDLY shows alternatives" "Safe Alternatives:" "$OUTPUT"

# Test 4: All policies accept valid writes
echo ""
echo "Test 4: All policies accept valid writes"
for POLICY in STRICT SAFE_DEFAULT DEV_FRIENDLY; do
    OUTPUT=$(cat << EOF | ./build/redis_db 2>&1
GUARD ADD RANGE_INT test_guard test* 0 100
POLICY SET $POLICY
PROPOSE SET test 50
EXIT
EOF
)
    check_result "$POLICY accepts valid write" "ACCEPT ✓" "$OUTPUT"
done

# Test 5: Policy persistence
echo ""
echo "Test 5: Policy getter reflects setter"
OUTPUT=$(cat << 'EOF' | ./build/redis_db 2>&1
POLICY SET STRICT
POLICY GET
EXIT
EOF
)
check_result "GET shows STRICT" "STRICT" "$OUTPUT"

OUTPUT=$(cat << 'EOF' | ./build/redis_db 2>&1
POLICY SET DEV_FRIENDLY
POLICY GET
EXIT
EOF
)
check_result "GET shows DEV_FRIENDLY" "DEV_FRIENDLY" "$OUTPUT"

# Test 6: Default policy is SAFE_DEFAULT
echo ""
echo "Test 6: Default policy"
OUTPUT=$(cat << 'EOF' | ./build/redis_db 2>&1
POLICY GET
EXIT
EOF
)
check_result "Default is SAFE_DEFAULT" "SAFE_DEFAULT" "$OUTPUT"

# Summary
echo ""
echo "========================================"
echo "  Test Summary"
echo "========================================"
echo "Passed: $TEST_PASSED"
echo "Failed: $TEST_FAILED"
echo ""

if [ $TEST_FAILED -eq 0 ]; then
    echo "✓ All tests passed!"
    exit 0
else
    echo "✗ Some tests failed"
    exit 1
fi
