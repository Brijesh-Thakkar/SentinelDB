# Temporal Query Quick Reference

## Command Summary

| Command | Description | Example |
|---------|-------------|---------|
| `SET key value` | Create new version | `SET price 100` |
| `GET key` | Get latest value | `GET price` |
| `GET key AT <timestamp>` | Query historical value | `GET price AT 1738467139567` |
| `EXPLAIN GET key AT <timestamp>` | Explain version selection | `EXPLAIN GET price AT 2026-02-02 14:30:00` |
| `HISTORY key` | Show all versions | `HISTORY price` |
| `DEL key` | Delete all versions | `DEL price` |
| `PROPOSE SET key value` | Evaluate write (with guards) | `PROPOSE SET score 150` |
| `GUARD ADD <type> ...` | Add constraint guard | `GUARD ADD RANGE_INT score_guard score* 0 100` |
| `GUARD LIST` | List all guards | `GUARD LIST` |
| `GUARD REMOVE <name>` | Remove guard | `GUARD REMOVE score_guard` |
| `POLICY GET` | Show decision policy | `POLICY GET` |
| `POLICY SET <name>` | Set decision policy | `POLICY SET STRICT` |
| `CONFIG RETENTION <mode>` | Set retention policy | `CONFIG RETENTION LAST 5` |
| `SNAPSHOT` | Create snapshot | `SNAPSHOT` |
| `EXIT` | Quit application | `EXIT` |

## Decision Policies

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `STRICT` | Reject all violations | Production, compliance |
| `SAFE_DEFAULT` | Negotiate low-risk (default) | Most applications |
| `DEV_FRIENDLY` | Always negotiate | Development, learning |

Quick examples:
```
POLICY SET STRICT              # Maximum safety
POLICY SET SAFE_DEFAULT        # Balanced (default)
POLICY SET DEV_FRIENDLY        # Maximum guidance
POLICY GET                     # Show current policy
```

## Timestamp Formats

### Epoch Milliseconds
```
redis> GET price AT 1738467139567
"100"
```

### ISO-8601 (YYYY-MM-DD HH:MM:SS)
```
redis> GET price AT 2026-02-02 14:30:00
"150"
```

## Example Workflow

```bash
# Start database
./build/redis_db

# Track changes over time
redis> SET price 100
OK
redis> SET price 150
OK
redis> SET price 200
OK

# View complete history
redis> HISTORY price
3 version(s):
1) [2026-02-02 02:52:19.567] "100"
2) [2026-02-02 02:52:19.568] "150"
3) [2026-02-02 02:52:19.569] "200"

# Query at specific time
redis> GET price AT 1738467139567
"100"

# Get current value
redis> GET price
"200"

# Create snapshot for compaction
redis> SNAPSHOT
OK

# Exit
redis> EXIT
Goodbye!
```

## Common Use Cases

### 1. Price Tracking
Track product prices over time and query historical prices.

### 2. Configuration Audit
See what configuration values were active at any point in time.

### 3. Status History
Monitor state changes and debug issues by examining past states.

### 4. Data Recovery
Restore previous values if bad data was written.

## Tips

- **HISTORY** shows all versions with timestamps
- **GET AT** retrieves value at specific point in time
- Timestamps persist across restarts via WAL
- Use **SNAPSHOT** to compact history and reduce memory
- All temporal data survives application restarts

## Error Messages

| Message | Meaning |
|---------|---------|
| `(nil)` | Key not found or no version at that time |
| `(empty array)` | Key has no versions |
| `(error) ERR invalid timestamp format` | Timestamp not in epoch ms or ISO-8601 format |
| `(error) ERR wrong number of arguments` | Missing required arguments |

## Getting Help

- Full documentation: [README.md](README.md)
- Temporal features: [TEMPORAL.md](TEMPORAL.md)
- Demo script: `./demo_temporal.sh`
- Test programs: `./build/test_temporal`, `./build/test_wal_temporal`
