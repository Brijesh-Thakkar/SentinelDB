#!/bin/bash

echo "=== Temporal Retention Policy Testing ==="
echo ""

cd /home/brijesh-thakkar/Desktop/DSA_Project

# Test 1: LAST_N retention
echo "Test 1: LAST_N Retention (keep last 2 versions)"
echo "-----------------------------------------------"
rm -rf data/
cat << 'EOF' | ./build/redis_db 2>&1 | grep -E "(redis>|version|OK -)"
CONFIG RETENTION LAST_N 2
SET value v1
SET value v2
SET value v3
SET value v4
HISTORY value
GET value
EXIT
EOF
echo ""

# Test 2: FULL retention (default)
echo "Test 2: FULL Retention (keep all versions)"
echo "-------------------------------------------"
rm -rf data/
cat << 'EOF' | ./build/redis_db 2>&1 | grep -E "(redis>|version|OK -)"
SET value v1
SET value v2
SET value v3
SET value v4
HISTORY value
CONFIG RETENTION FULL
SET value v5
HISTORY value
EXIT
EOF
echo ""

# Test 3: Dynamic policy change
echo "Test 3: Dynamic Policy Change"
echo "------------------------------"
rm -rf data/
cat << 'EOF' | ./build/redis_db 2>&1 | grep -E "(redis>|version|OK -)"
SET item a
SET item b
SET item c
SET item d
SET item e
HISTORY item
CONFIG RETENTION LAST_N 3
HISTORY item
CONFIG RETENTION LAST_N 2
HISTORY item
EXIT
EOF
echo ""

# Test 4: Multiple keys with retention
echo "Test 4: Multiple Keys with Same Policy"
echo "---------------------------------------"
rm -rf data/
cat << 'EOF' | ./build/redis_db 2>&1 | grep -E "(redis>|version|OK -|key)"
CONFIG RETENTION LAST_N 2
SET key1 a
SET key1 b
SET key1 c
SET key2 x
SET key2 y
SET key2 z
HISTORY key1
HISTORY key2
EXIT
EOF
echo ""

# Test 5: Retention with WAL replay
echo "Test 5: Retention Persists After Restart"
echo "-----------------------------------------"
rm -rf data/
cat << 'EOF' | ./build/redis_db 2>&1 > /dev/null
CONFIG RETENTION LAST_N 2
SET price 100
SET price 150
SET price 200
SET price 250
EXIT
EOF

echo "After restart (retention should still apply to new versions):"
cat << 'EOF' | ./build/redis_db 2>&1 | grep -E "(redis>|version|Restored)"
SET price 300
HISTORY price
EXIT
EOF
echo ""

echo "=== All Tests Complete ==="
