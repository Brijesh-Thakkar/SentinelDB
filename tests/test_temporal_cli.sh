#!/bin/bash

# Clean up previous test data
rm -rf data/

echo "=== Testing Temporal Query Commands ==="
echo ""

# Create test commands
cat << 'EOF' > /tmp/temporal_test_commands.txt
SET price 100
SET price 150
SET price 200
GET price
HISTORY price
EXIT
EOF

echo "Test 1: Creating multiple versions of a key"
echo "Commands: SET price 100, SET price 150, SET price 200, GET price, HISTORY price"
echo "----------------------------------------"
./build/redis_db < /tmp/temporal_test_commands.txt
echo ""
echo ""

# Test with temporal queries
echo "=== Test 2: Temporal Queries with Timestamp ==="
echo ""

# Get the current timestamp in milliseconds for testing
TIMESTAMP=$(date +%s%3N)
echo "Current timestamp (epoch ms): $TIMESTAMP"
echo ""

# Create commands with a delay to get different timestamps
cat << 'EOF' > /tmp/temporal_test2.txt
SET user Alice
EOF

echo "Setting initial value..."
./build/redis_db < /tmp/temporal_test2.txt > /dev/null
sleep 1

# Record timestamp after first value
TIMESTAMP1=$(date +%s%3N)
echo "Timestamp after first SET: $TIMESTAMP1"

cat << 'EOF' >> /tmp/temporal_test2.txt
SET user Bob
EOF

echo "Setting second value..."
sleep 1

# Record timestamp after second value
TIMESTAMP2=$(date +%s%3N)
echo "Timestamp after second SET: $TIMESTAMP2"

cat << 'EOF' >> /tmp/temporal_test2.txt
SET user Charlie
EXIT
EOF

echo "Setting third value..."
./build/redis_db < /tmp/temporal_test2.txt > /dev/null

# Now query at different times
cat << EOF > /tmp/temporal_query.txt
HISTORY user
GET user AT $TIMESTAMP1
GET user AT $TIMESTAMP2
GET user
EXIT
EOF

echo ""
echo "Querying history and specific timestamps:"
echo "----------------------------------------"
./build/redis_db < /tmp/temporal_query.txt

echo ""
echo "=== Test 3: ISO-8601 Format ==="
# Create a readable timestamp
ISO_TIME=$(date -d "5 minutes ago" "+%Y-%m-%d %H:%M:%S")
echo "Querying with ISO-8601 format: $ISO_TIME"
echo "----------------------------------------"

cat << EOF > /tmp/iso_test.txt
SET weather sunny
HISTORY weather
GET weather AT $ISO_TIME
EXIT
EOF

./build/redis_db < /tmp/iso_test.txt

# Cleanup
rm -f /tmp/temporal_test_commands.txt /tmp/temporal_test2.txt /tmp/temporal_query.txt /tmp/iso_test.txt

echo ""
echo "=== Test Complete ==="
