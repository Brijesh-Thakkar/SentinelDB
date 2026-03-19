#!/bin/bash

echo "=== Testing GET AT Command ==="
echo ""

cd /home/brijesh-thakkar/Desktop/DSA_Project
rm -rf data/

echo "Step 1: Creating versions with delays..."
echo ""

# Create first version and capture timestamp
cat << 'EOF' | ./build/redis_db > /dev/null 2>&1
SET price 100
EXIT
EOF
sleep 1

# Create second version
cat << 'EOF' | ./build/redis_db > /dev/null 2>&1
SET price 150
EXIT
EOF
sleep 1

# Create third version
cat << 'EOF' | ./build/redis_db > /dev/null 2>&1
SET price 200
EXIT
EOF

echo "Step 2: Viewing history and testing queries..."
echo ""

cat << 'EOF' | ./build/redis_db
HISTORY price
EXIT
EOF

echo ""
echo "Now you can copy a timestamp from above and test:"
echo "  redis> GET price AT <paste-timestamp-here>"
echo ""
echo "Or try with epoch milliseconds:"
CURRENT_MS=$(date +%s%3N)
PAST_MS=$((CURRENT_MS - 2000))
echo "  redis> GET price AT $PAST_MS"
echo ""
echo "Or without milliseconds (will work too):"
echo "  redis> GET price AT 2026-02-02 03:00:28"
echo ""
