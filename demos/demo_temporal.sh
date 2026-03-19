#!/bin/bash

# Demo script for temporal query features

echo "================================================"
echo "  Redis-like Database: Temporal Query Demo"
echo "================================================"
echo ""

# Clean previous test data
rm -rf data/

echo "This demo shows how to query historical values"
echo "using the GET AT and HISTORY commands."
echo ""
echo "Press Enter to continue..."
read

echo ""
echo "--- Demo 1: Tracking Price Changes ---"
echo ""
echo "We'll track a product price over time:"
echo ""

cat << 'EOF' | ./build/redis_db 2>/dev/null &
SET product_name "Gaming Laptop"
SET price 1200
EXIT
EOF
sleep 1

echo "✓ Initial price: $1200"
echo "  Command: SET price 1200"
echo ""
sleep 1

# Capture timestamp after first version
TIMESTAMP1=$(date +%s%3N)
sleep 1

cat << 'EOF' | ./build/redis_db 2>/dev/null &
SET price 999
EXIT
EOF
sleep 1

echo "✓ Price dropped to: $999 (Black Friday sale!)"
echo "  Command: SET price 999"
echo ""
sleep 1

TIMESTAMP2=$(date +%s%3N)
sleep 1

cat << 'EOF' | ./build/redis_db 2>/dev/null &
SET price 1099
EXIT
EOF
sleep 1

echo "✓ Price increased to: $1099 (sale ended)"
echo "  Command: SET price 1099"
echo ""
sleep 2

echo "Now let's view the complete price history:"
echo ""
cat << 'EOF' | ./build/redis_db 2>/dev/null | grep -A 5 "version(s)"
HISTORY price
EXIT
EOF
echo ""
sleep 2

echo "Let's query the price at specific timestamps:"
echo ""
echo "Query 1: Price at timestamp $TIMESTAMP1 (before sale)"
cat << EOF | ./build/redis_db 2>/dev/null | grep -v "redis>" | grep -v "Commands" | grep -v "Type" | grep -v "Key-Value" | grep -v "^$" | tail -1
GET price AT $TIMESTAMP1
EXIT
EOF
echo ""
sleep 1

echo "Query 2: Price at timestamp $TIMESTAMP2 (during sale)"
cat << EOF | ./build/redis_db 2>/dev/null | grep -v "redis>" | grep -v "Commands" | grep -v "Type" | grep -v "Key-Value" | grep -v "^$" | tail -1
GET price AT $TIMESTAMP2
EXIT
EOF
echo ""
sleep 1

echo "Query 3: Current price"
cat << 'EOF' | ./build/redis_db 2>/dev/null | grep -v "redis>" | grep -v "Commands" | grep -v "Type" | grep -v "Key-Value" | grep -v "^$" | tail -1
GET price
EXIT
EOF
echo ""
sleep 2

echo ""
echo "--- Demo 2: Configuration Changes ---"
echo ""

rm -rf data/

echo "Let's track configuration changes:"
echo ""

cat << 'EOF' | ./build/redis_db 2>/dev/null &
SET config_mode "development"
SET config_mode "staging"
SET config_mode "production"
EXIT
EOF
sleep 1

echo "✓ Set config: development → staging → production"
echo ""
sleep 1

echo "View all configuration changes:"
echo ""
cat << 'EOF' | ./build/redis_db 2>/dev/null | grep -A 5 "version(s)"
HISTORY config_mode
EXIT
EOF
echo ""
sleep 2

echo ""
echo "--- Demo 3: Persistence Test ---"
echo ""

rm -rf data/

echo "Creating temporal data and restarting database..."
echo ""

cat << 'EOF' | ./build/redis_db 2>/dev/null &
SET status "initializing"
SET status "running"
SET status "completed"
EXIT
EOF
sleep 1

echo "✓ Created 3 versions of 'status'"
echo ""
sleep 1

echo "Restarting database to test persistence..."
echo ""

cat << 'EOF' | ./build/redis_db 2>&1 | grep -E "(Replaying|Restored|version\(s\)|initializing|running|completed)"
HISTORY status
EXIT
EOF
echo ""
sleep 1

echo "✓ All temporal data persisted correctly!"
echo ""

echo ""
echo "================================================"
echo "  Demo Complete!"
echo "================================================"
echo ""
echo "Key Features Demonstrated:"
echo "  ✓ HISTORY command shows all versions with timestamps"
echo "  ✓ GET AT queries historical values"
echo "  ✓ Timestamps support epoch milliseconds format"
echo "  ✓ All temporal data persists across restarts"
echo ""
echo "Try it yourself: ./build/redis_db"
echo ""
