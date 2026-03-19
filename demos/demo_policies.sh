#!/bin/bash
# Interactive Demo: Decision Policies in Write Evaluation
# This script demonstrates the three decision policy modes

echo "===================================================================="
echo "  Decision Policy Demo - Temporal KV Store with Write Evaluation"
echo "===================================================================="
echo ""
echo "This demo shows how different policies handle guard violations:"
echo "  • STRICT: Reject all violations, no alternatives"
echo "  • SAFE_DEFAULT: Negotiate low-risk, reject high-risk"
echo "  • DEV_FRIENDLY: Always negotiate when alternatives exist"
echo ""
echo "Press Enter to continue..."
read

# Create test commands
cat > /tmp/policy_demo.txt << 'EOF'
# Setup: Create guards
GUARD ADD RANGE_INT score_guard score* 0 100
GUARD ADD ENUM status_guard status* active,inactive,pending

# ===== Test 1: STRICT Policy =====
POLICY SET STRICT
POLICY GET
PROPOSE SET score 150

# ===== Test 2: SAFE_DEFAULT Policy =====
POLICY SET SAFE_DEFAULT
POLICY GET
PROPOSE SET score 150

# ===== Test 3: DEV_FRIENDLY Policy =====
POLICY SET DEV_FRIENDLY
POLICY GET
PROPOSE SET score 150

# ===== Compare with enum guard =====
POLICY SET STRICT
PROPOSE SET status invalid

POLICY SET SAFE_DEFAULT
PROPOSE SET status invalid

POLICY SET DEV_FRIENDLY
PROPOSE SET status invalid

# Reset
POLICY SET SAFE_DEFAULT
EXIT
EOF

./build/redis_db < /tmp/policy_demo.txt
rm /tmp/policy_demo.txt
