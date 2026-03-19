#!/bin/bash
# Quick Demo: Decision Policies in Action
# Shows the three policies side-by-side with same violation

clear
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║          Decision Policy Demo - Temporal KV Store                   ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "This demo shows how the three policies handle the SAME guard violation:"
echo ""
echo "  Scenario: Score value 150 violates range [0, 100]"
echo ""
echo "Press Enter to see each policy's response..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " 1️⃣  STRICT Policy: \"Reject first, ask questions never\""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cat << 'EOF' | ./build/redis_db 2>&1 | grep -A 15 "WRITE EVALUATION" | head -20
GUARD ADD RANGE_INT score_guard score* 0 100
POLICY SET STRICT
PROPOSE SET score 150
EXIT
EOF

echo ""
echo "Press Enter for next policy..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " 2️⃣  SAFE_DEFAULT Policy: \"Help when safe, reject when risky\""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cat << 'EOF' | ./build/redis_db 2>&1 | grep -A 20 "WRITE EVALUATION" | head -25
GUARD ADD RANGE_INT score_guard score* 0 100
POLICY SET SAFE_DEFAULT
PROPOSE SET score 150
EXIT
EOF

echo ""
echo "Press Enter for next policy..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " 3️⃣  DEV_FRIENDLY Policy: \"Maximum developer assistance\""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cat << 'EOF' | ./build/redis_db 2>&1 | grep -A 20 "WRITE EVALUATION" | head -25
GUARD ADD RANGE_INT score_guard score* 0 100
POLICY SET DEV_FRIENDLY
PROPOSE SET score 150
EXIT
EOF

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                         Key Takeaways                                ║"
echo "╠══════════════════════════════════════════════════════════════════════╣"
echo "║                                                                      ║"
echo "║  • STRICT: Rejects immediately, no alternatives shown                ║"
echo "║    → Best for: Production, compliance-critical systems              ║"
echo "║                                                                      ║"
echo "║  • SAFE_DEFAULT: Shows alternatives when safe to negotiate           ║"
echo "║    → Best for: Most applications (balanced approach)                ║"
echo "║                                                                      ║"
echo "║  • DEV_FRIENDLY: Always shows alternatives and guidance              ║"
echo "║    → Best for: Development, learning, debugging                     ║"
echo "║                                                                      ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Try it yourself:"
echo "  ./build/redis_db"
echo "  > GUARD ADD RANGE_INT score* 0 100"
echo "  > POLICY SET STRICT"
echo "  > PROPOSE SET score 150"
echo ""
