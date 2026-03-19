# Decision Policies Guide

## Overview

Decision policies control how the write evaluation system handles guard violations. They determine whether violations are rejected immediately, negotiated with alternatives, or handled based on risk assessment.

## Policy Hierarchy

```
┌─────────────────────────────────────────────────┐
│              Write Proposal                      │
│         (PROPOSE SET key value)                  │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│          Guard Evaluation Layer                  │
│   (Guards validate against constraints)          │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│         Decision Policy Layer                    │
│  (Policy modifies result based on mode)          │
│                                                   │
│  • STRICT → Always reject violations            │
│  • SAFE_DEFAULT → Risk-based decision           │
│  • DEV_FRIENDLY → Always negotiate              │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│              Final Decision                      │
│    ACCEPT ✓ | REJECT ✗ | COUNTER_OFFER ⚠       │
└─────────────────────────────────────────────────┘
```

## The Three Policies

### 1. STRICT Policy

**Philosophy**: "Reject first, ask questions never"

**Behavior**:
- All guard violations are immediately rejected
- No alternatives shown or generated
- No negotiation possible
- Maximum safety and compliance

**When to use**:
- Production systems with zero-tolerance requirements
- Financial data with regulatory compliance
- Safety-critical systems
- When any constraint violation is unacceptable

**Example**:
```
redis> POLICY SET STRICT
redis> GUARD ADD RANGE_INT score_guard score* 0 100
redis> PROPOSE SET score 150

========== WRITE EVALUATION ==========
Proposal:  SET "score" "150"
Result:    REJECT ✗
Reason:    Value 150 outside acceptable range [0, 100]
Triggered: score_guard

This write cannot be performed.
======================================
```

**Key Points**:
- ✅ Maximum safety
- ✅ Clear compliance boundaries
- ❌ No developer assistance
- ❌ Requires exact values

---

### 2. SAFE_DEFAULT Policy (Default)

**Philosophy**: "Help when safe, reject when risky"

**Behavior**:
- Evaluates if safe alternatives exist
- If alternatives available → COUNTER_OFFER (negotiate)
- If no alternatives → REJECT (too risky)
- Balanced approach between safety and usability

**Decision Logic**:
```cpp
if (guardsFailed) {
    if (alternatives.empty()) {
        return REJECT;  // High-risk: no clear path forward
    } else {
        return COUNTER_OFFER;  // Low-risk: can guide developer
    }
}
return ACCEPT;
```

**When to use**:
- Most production applications
- Balanced environments
- When you want safety with reasonable developer support
- Default choice for new projects

**Example with alternatives**:
```
redis> POLICY SET SAFE_DEFAULT
redis> PROPOSE SET score 150

Result: COUNTER_OFFER ⚠
Safe Alternatives:
  1) "100" → Maximum allowed value
  2) "75"  → Conservative value within range
```

**Example without alternatives**:
```
redis> PROPOSE SET score NaN

Result: REJECT ✗
Reason: Cannot generate safe alternatives for non-numeric value
```

**Key Points**:
- ✅ Balanced safety and usability
- ✅ Guides developers when possible
- ✅ Fails safe when uncertain
- ✅ Good default choice

---

### 3. DEV_FRIENDLY Policy

**Philosophy**: "Maximum developer assistance and guidance"

**Behavior**:
- Always shows alternatives when guard evaluation generates them
- Never upgrades COUNTER_OFFER to REJECT
- Provides maximum information to developers
- Optimized for learning and exploration

**When to use**:
- Development environments
- Learning the system
- Debugging guard configurations
- Rapid prototyping
- Testing constraint behavior

**Example**:
```
redis> POLICY SET DEV_FRIENDLY
redis> GUARD ADD ENUM status_guard status* active,inactive,pending
redis> PROPOSE SET status invalid

========== WRITE EVALUATION ==========
Proposal:  SET "status" "invalid"
Result:    COUNTER_OFFER ⚠
Reason:    Value 'invalid' not in allowed set
Triggered: status_guard

Safe Alternatives:
  1) "active"    → Allowed value
  2) "inactive"  → Allowed value
  3) "pending"   → Allowed value
======================================
```

**Key Points**:
- ✅ Maximum guidance
- ✅ Shows all available alternatives
- ✅ Great for learning
- ❌ Less strict enforcement
- ❌ Not recommended for production

---

## Policy Comparison

### Decision Matrix

| Guard Result | Alternatives | STRICT | SAFE_DEFAULT | DEV_FRIENDLY |
|--------------|-------------|--------|--------------|--------------|
| ACCEPT | N/A | ACCEPT ✓ | ACCEPT ✓ | ACCEPT ✓ |
| COUNTER_OFFER | Yes | REJECT ✗ | COUNTER_OFFER ⚠ | COUNTER_OFFER ⚠ |
| COUNTER_OFFER | No | REJECT ✗ | REJECT ✗ | REJECT ✗ |
| REJECT | N/A | REJECT ✗ | REJECT ✗ | REJECT ✗ |

### Alternatives Display

| Policy | Shows Alternatives |
|--------|-------------------|
| STRICT | Never |
| SAFE_DEFAULT | Only when negotiating |
| DEV_FRIENDLY | Always (when generated) |

### Environment Recommendations

| Environment | Recommended Policy | Reason |
|-------------|-------------------|---------|
| Production | STRICT or SAFE_DEFAULT | Safety-critical |
| Staging | SAFE_DEFAULT | Balance testing and safety |
| Development | DEV_FRIENDLY | Maximum learning |
| CI/CD | STRICT | Enforce compliance |
| Debugging | DEV_FRIENDLY | Maximum information |

---

## Policy Commands

### Get Current Policy
```
POLICY GET
```
Shows the active policy with description.

**Output**:
```
Current decision policy: SAFE_DEFAULT
Description: Negotiate low-risk violations (with alternatives), reject high-risk (no alternatives)
```

### Set Policy
```
POLICY SET <policy_name>
```

**Available policies**:
- `STRICT`
- `SAFE_DEFAULT`
- `DEV_FRIENDLY`

**Example**:
```
redis> POLICY SET DEV_FRIENDLY
OK - Decision policy set to DEV_FRIENDLY
```

---

## Advanced Scenarios

### Scenario 1: Range Violation

**Setup**:
```
GUARD ADD RANGE_INT age_guard age* 0 120
```

| Proposed Value | STRICT | SAFE_DEFAULT | DEV_FRIENDLY |
|---------------|--------|--------------|--------------|
| `50` | ACCEPT ✓ | ACCEPT ✓ | ACCEPT ✓ |
| `150` | REJECT ✗ | COUNTER_OFFER ⚠ (shows 120, 60) | COUNTER_OFFER ⚠ |
| `-10` | REJECT ✗ | COUNTER_OFFER ⚠ (shows 0, 60) | COUNTER_OFFER ⚠ |
| `abc` | REJECT ✗ | REJECT ✗ | REJECT ✗ |

### Scenario 2: Enum Violation

**Setup**:
```
GUARD ADD ENUM level_guard level* low,medium,high
```

| Proposed Value | STRICT | SAFE_DEFAULT | DEV_FRIENDLY |
|---------------|--------|--------------|--------------|
| `medium` | ACCEPT ✓ | ACCEPT ✓ | ACCEPT ✓ |
| `critical` | REJECT ✗ | COUNTER_OFFER ⚠ (shows all 3) | COUNTER_OFFER ⚠ |
| `123` | REJECT ✗ | COUNTER_OFFER ⚠ (shows all 3) | COUNTER_OFFER ⚠ |

### Scenario 3: Length Violation

**Setup**:
```
GUARD ADD LENGTH name_guard name* 3 20
```

| Proposed Value | STRICT | SAFE_DEFAULT | DEV_FRIENDLY |
|---------------|--------|--------------|--------------|
| `Alice` | ACCEPT ✓ | ACCEPT ✓ | ACCEPT ✓ |
| `ab` | REJECT ✗ | COUNTER_OFFER ⚠ (shows "ab*") | COUNTER_OFFER ⚠ |
| `verylongname123456789abc` | REJECT ✗ | COUNTER_OFFER ⚠ (shows truncated) | COUNTER_OFFER ⚠ |

---

## Policy Internals

### Implementation Flow

```cpp
WriteEvaluation proposeSet(key, value) {
    // 1. Simulate write (guard evaluation)
    WriteEvaluation eval = simulateWrite(key, value);
    
    // 2. Apply decision policy
    applyDecisionPolicy(eval);
    
    // 3. Add policy metadata
    eval.appliedPolicy = currentPolicy;
    eval.policyReasoning = "Policy: " + policyName;
    
    return eval;
}
```

### Policy Application Logic

```cpp
void applyDecisionPolicy(WriteEvaluation& eval) {
    if (eval.result == GuardResult::ACCEPT) {
        return;  // No policy needed for successful writes
    }
    
    switch (decisionPolicy) {
        case STRICT:
            // Always reject violations
            if (eval.result == GuardResult::COUNTER_OFFER) {
                eval.result = GuardResult::REJECT;
                eval.alternatives.clear();
            }
            break;
            
        case SAFE_DEFAULT:
            // Reject if no safe alternatives
            if (eval.result == GuardResult::COUNTER_OFFER && 
                eval.alternatives.empty()) {
                eval.result = GuardResult::REJECT;
            }
            break;
            
        case DEV_FRIENDLY:
            // Keep COUNTER_OFFER, show all alternatives
            break;
    }
}
```

### Key Design Principles

1. **Policy does NOT change guard evaluation**: Guards always evaluate the same way
2. **Policy modifies the decision**: Only affects final result and alternatives
3. **Deterministic**: Same input always produces same output for a given policy
4. **Explainable**: Policy reasoning is always recorded
5. **Composable**: Works with all guard types

---

## Best Practices

### 1. Start Strict, Relax Gradually
```
# Start with maximum safety
POLICY SET STRICT

# Test and validate
PROPOSE SET key value

# Relax only after understanding constraints
POLICY SET SAFE_DEFAULT
```

### 2. Use DEV_FRIENDLY for Learning
```
# Learn what guards do
POLICY SET DEV_FRIENDLY
GUARD ADD RANGE_INT score* 0 100
PROPOSE SET score 50    # See it accept
PROPOSE SET score 150   # See alternatives
PROPOSE SET score -10   # See lower bound
```

### 3. Environment-Specific Policies
```bash
# Set policy based on environment
if [ "$ENV" = "production" ]; then
  echo "POLICY SET STRICT"
elif [ "$ENV" = "staging" ]; then
  echo "POLICY SET SAFE_DEFAULT"
else
  echo "POLICY SET DEV_FRIENDLY"
fi | ./redis_db
```

### 4. Policy as Feature Flag
```
# Gradual rollout
# Week 1: DEV_FRIENDLY (collect data)
# Week 2: SAFE_DEFAULT (validate)
# Week 3: STRICT (enforce)
```

---

## Troubleshooting

### Issue: "Everything is rejected"
**Solution**: Check policy setting
```
POLICY GET
# If STRICT, consider switching to SAFE_DEFAULT
POLICY SET SAFE_DEFAULT
```

### Issue: "Not seeing alternatives"
**Solution**: Use DEV_FRIENDLY or SAFE_DEFAULT
```
POLICY SET DEV_FRIENDLY  # Always shows alternatives
POLICY GET               # Verify setting
```

### Issue: "Need strict enforcement"
**Solution**: Use STRICT policy
```
POLICY SET STRICT
# Now all violations are rejected
```

---

## Related Documentation

- [README.md](README.md) - Project overview
- [TEMPORAL.md](TEMPORAL.md) - Temporal versioning
- [RETENTION.md](RETENTION.md) - Retention policies
- [QUICKREF.md](QUICKREF.md) - Command reference

---

## Summary

| Policy | Safety | Usability | Environment | Default |
|--------|--------|-----------|-------------|---------|
| STRICT | ⭐⭐⭐⭐⭐ | ⭐ | Production | No |
| SAFE_DEFAULT | ⭐⭐⭐⭐ | ⭐⭐⭐ | Most apps | **Yes** |
| DEV_FRIENDLY | ⭐⭐ | ⭐⭐⭐⭐⭐ | Development | No |

**Quick Decision Tree**:
```
Production system?
├─ Yes → STRICT or SAFE_DEFAULT
└─ No → DEV_FRIENDLY

Need compliance?
├─ Yes → STRICT
└─ No → SAFE_DEFAULT

Learning/Debugging?
└─ DEV_FRIENDLY
```
