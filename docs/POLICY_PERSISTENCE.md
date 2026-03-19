# Policy Persistence and HTTP Control

## Overview

Decision policies in SentinelDB are now fully persistent and remotely controllable via HTTP API. This document describes the implementation and usage.

## Features Implemented

### 1. Policy Persistence via WAL

**How it works:**
- When a policy is changed (via CLI or HTTP), a `POLICY SET <name>` command is appended to the WAL
- During startup, policy commands are replayed BEFORE data commands
- The last `POLICY SET` command determines the active policy

**WAL Format:**
```
POLICY SET STRICT
POLICY SET DEV_FRIENDLY
POLICY SET SAFE_DEFAULT
```

**Example:**
```bash
# Session 1: Set policy
redis> POLICY SET STRICT
OK - Decision policy set to STRICT

# Session 2: Restart and verify
redis> POLICY GET
Current decision policy: STRICT
Description: Reject all guard violations without negotiation
```

### 2. Policy Persistence via Snapshots

**How it works:**
- Snapshots include the current policy as the first line
- On snapshot load, policy is restored before loading data
- If both snapshot and WAL exist, WAL policy commands take precedence

**Snapshot Format:**
```
POLICY SET DEV_FRIENDLY
SET key1 value1
SET key2 value2
```

**Example:**
```bash
redis> POLICY SET DEV_FRIENDLY
redis> SET key1 value1
redis> SNAPSHOT
OK

# After restart (from snapshot only):
redis> POLICY GET
Current decision policy: DEV_FRIENDLY
```

### 3. CLI Commands

**POLICY GET** - Display current policy
```bash
redis> POLICY GET
Current decision policy: SAFE_DEFAULT
Description: Negotiate low-risk violations (with alternatives), reject high-risk (no alternatives)
```

**POLICY SET <name>** - Change active policy
```bash
redis> POLICY SET STRICT
OK - Decision policy set to STRICT

redis> POLICY SET DEV_FRIENDLY
OK - Decision policy set to DEV_FRIENDLY

redis> POLICY SET SAFE_DEFAULT
OK - Decision policy set to SAFE_DEFAULT
```

**Error Handling:**
```bash
redis> POLICY SET INVALID
(error) ERR unknown policy 'INVALID'
Available policies: DEV_FRIENDLY, SAFE_DEFAULT, STRICT
```

### 4. HTTP API

**GET /policy** - Get current policy

Request:
```bash
curl http://localhost:8080/policy
```

Response:
```json
{
  "activePolicy": "SAFE_DEFAULT",
  "description": "Negotiate low-risk violations (with alternatives), reject high-risk (no alternatives)"
}
```

**POST /policy** - Set policy

Request:
```bash
curl -X POST http://localhost:8080/policy \
  -H "Content-Type: application/json" \
  -d '{"policy":"STRICT"}'
```

Response:
```json
{
  "status": "ok",
  "activePolicy": "STRICT"
}
```

**Error Responses:**

Invalid policy:
```json
{
  "error": "Invalid policy. Use DEV_FRIENDLY, SAFE_DEFAULT, or STRICT"
}
```

Missing parameter:
```json
{
  "error": "Missing 'policy' parameter"
}
```

### 5. Policy-Aware Explanations

All rejection messages now explicitly mention the policy name:

**STRICT Policy:**
```
Result:    REJECT ✗
Reason:    Value 150 outside acceptable range [0, 100]
           Rejected under STRICT policy due to guard violation
```

**SAFE_DEFAULT Policy:**
```
Result:    REJECT ✗
Reason:    Value cannot be determined
           Rejected under SAFE_DEFAULT policy - no safe alternatives available
```

**DEV_FRIENDLY Policy:**
```
Result:    COUNTER_OFFER ⚠
Reason:    Value 150 outside acceptable range [0, 100]
           Counter-offer under DEV_FRIENDLY policy - showing alternatives
```

## Implementation Details

### KVStore Changes

**New Behavior:**
- `setDecisionPolicy()` now logs to WAL automatically
- Policy is stored in-memory and persisted via WAL
- Policy is included in snapshots

**Code:**
```cpp
void KVStore::setDecisionPolicy(DecisionPolicy policy) {
    decisionPolicy = policy;
    
    // Log policy change to WAL
    if (walEnabled && wal && wal->isEnabled()) {
        std::string policyName = /* convert policy to string */;
        wal->logPolicy(policyName);
    }
}
```

### WAL Replay

**Startup Sequence:**
1. Initialize WAL
2. Load snapshot (if exists)
   - Parse POLICY commands first
   - Then parse data commands
3. Replay WAL
   - **Phase 1:** Replay all POLICY commands (in order)
   - **Phase 2:** Replay all data commands
4. Re-enable WAL logging

**Why two phases?**
Policy commands must be replayed before data commands to ensure the correct policy is active when evaluating guard constraints during recovery.

### HTTP Server Initialization

**New Behavior:**
- HTTP server now replays WAL on startup
- Policy is restored before serving requests
- WAL replay is automatic when `--wal` flag is provided

**Code Flow:**
```cpp
if (wal && wal->isEnabled()) {
    kvstore->setWalEnabled(false);
    
    // Replay snapshot
    for (auto& cmd : snapshotCommands) {
        if (isPolicyCommand) applyPolicy();
        else if (isDataCommand) applyData();
    }
    
    // Replay WAL (policies first, then data)
    for (auto& cmd : walCommands) {
        if (isPolicyCommand) applyPolicy();
    }
    for (auto& cmd : walCommands) {
        if (isDataCommand) applyData();
    }
    
    kvstore->setWalEnabled(true);
}
```

## Testing

### CLI Persistence Test

```bash
./test_policy_persistence.sh
```

Tests:
- ✓ WAL contains POLICY SET commands
- ✓ Policy restored after restart
- ✓ Last policy wins (multiple changes)
- ✓ Snapshot contains policy
- ✓ Policy restored from snapshot
- ✓ Default is SAFE_DEFAULT

### HTTP Persistence Test

```bash
./test_http_policy.sh
```

Tests:
- ✓ GET /policy returns default
- ✓ POST /policy sets policy
- ✓ Policy persists across HTTP server restarts
- ✓ Invalid policy returns error
- ✓ Missing parameter returns error

### Comprehensive Demo

```bash
./demo_policy_complete.sh
```

Demonstrates:
- Policy persistence via WAL
- Policy persistence via snapshots
- CLI commands
- HTTP endpoints
- HTTP persistence
- Policy-aware explanations

## Examples

### Example 1: CLI Workflow

```bash
# Start database
./build/redis_db

# Set strict policy for production
redis> POLICY SET STRICT
OK - Decision policy set to STRICT

# Add constraints
redis> GUARD ADD RANGE_INT score* 0 100

# Test write (will be rejected)
redis> PROPOSE SET score 150
Result: REJECT ✗
Reason: Rejected under STRICT policy due to guard violation

# Create snapshot
redis> SNAPSHOT
OK

# Restart (policy is restored)
# ...

redis> POLICY GET
Current decision policy: STRICT
```

### Example 2: HTTP Workflow

```bash
# Start HTTP server with WAL
./build/http_server --port 8080 --wal data/wal.log

# Check current policy
curl http://localhost:8080/policy
# {"activePolicy":"SAFE_DEFAULT",...}

# Set to strict for production
curl -X POST http://localhost:8080/policy \
  -H "Content-Type: application/json" \
  -d '{"policy":"STRICT"}'

# Restart server (policy is restored from WAL)
# ...

# Verify policy persisted
curl http://localhost:8080/policy
# {"activePolicy":"STRICT",...}
```

### Example 3: Environment-Specific Policies

```bash
# Development: Use DEV_FRIENDLY
curl -X POST http://dev-db:8080/policy \
  -d '{"policy":"DEV_FRIENDLY"}'

# Staging: Use SAFE_DEFAULT
curl -X POST http://staging-db:8080/policy \
  -d '{"policy":"SAFE_DEFAULT"}'

# Production: Use STRICT
curl -X POST http://prod-db:8080/policy \
  -d '{"policy":"STRICT"}'

# Each environment's policy persists across restarts
```

## Migration Guide

### Existing Databases

If you have an existing database without policy persistence:

1. **Default behavior:** Database will start with SAFE_DEFAULT policy
2. **To set persistent policy:** Use `POLICY SET <name>` or HTTP POST
3. **No data migration needed:** Existing data is unaffected

### Upgrading from Previous Versions

1. Stop the database
2. Update binaries
3. Restart - WAL will be replayed normally
4. Set desired policy (will be persisted going forward)

## API Reference

### CLI Commands

| Command | Arguments | Description |
|---------|-----------|-------------|
| `POLICY GET` | None | Display current policy and description |
| `POLICY SET` | `<DEV_FRIENDLY\|SAFE_DEFAULT\|STRICT>` | Change active policy |

### HTTP Endpoints

| Method | Path | Request Body | Response |
|--------|------|--------------|----------|
| GET | `/policy` | - | `{"activePolicy": "...", "description": "..."}` |
| POST | `/policy` | `{"policy": "..."}` | `{"status": "ok", "activePolicy": "..."}` |

## Troubleshooting

**Q: Policy not persisting after restart**
A: Ensure WAL is enabled. Check that `data/wal.log` exists and contains `POLICY SET` commands.

**Q: HTTP server not restoring policy**
A: Make sure you're using `--wal` flag when starting the server.

**Q: Multiple policy commands in WAL**
A: This is normal. The last `POLICY SET` command determines the active policy.

**Q: Policy different after snapshot**
A: WAL commands override snapshot. If you created a snapshot with policy A, then changed to policy B via WAL, policy B will be active.

## Performance Impact

- **WAL write:** One additional line per policy change (negligible)
- **Snapshot:** One additional line (first line of snapshot)
- **Startup:** Minimal overhead (policy commands replayed first)
- **Runtime:** No impact (policy is in-memory)

## Security Considerations

**Note:** This implementation does NOT include authentication or authorization.

For production use:
- Use network-level controls (firewall, VPN)
- Add authentication middleware if exposing HTTP API
- Audit policy changes via WAL
- Set restrictive policies (STRICT) in production

## Related Documentation

- [POLICIES.md](POLICIES.md) - Complete policy guide
- [README.md](README.md) - Project overview
- [TEMPORAL.md](TEMPORAL.md) - Temporal features
