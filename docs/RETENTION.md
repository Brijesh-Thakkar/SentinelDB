# Temporal Retention Policies

## Overview

The database supports configurable retention policies to control how many versions are kept per key. This helps manage memory usage while maintaining temporal query capabilities.

## Retention Modes

### FULL (Default)
Keeps all versions of every key.

```
redis> CONFIG RETENTION FULL
OK - Retention policy set to FULL (keep all versions)
```

### LAST N
Keeps only the last N versions per key. Older versions are automatically removed.

```
redis> CONFIG RETENTION LAST 5
OK - Retention policy set to LAST 5 (keep last 5 versions)
```

### LAST Ts
Keeps only versions created within the last T seconds. Versions older than T seconds are automatically removed.

```
redis> CONFIG RETENTION LAST 3600s
OK - Retention policy set to LAST 3600s (keep versions from last 3600 seconds)
```

## Usage Examples

### Example 1: Limit Version Count
```
redis> CONFIG RETENTION LAST 3
OK - Retention policy set to LAST 3 (keep last 3 versions)

redis> SET price 100
OK
redis> SET price 150
OK
redis> SET price 200
OK
redis> SET price 250
OK

redis> HISTORY price
3 version(s):
1) [2026-02-02 03:25:24.134] "150"
2) [2026-02-02 03:25:24.134] "200"
3) [2026-02-02 03:25:24.134] "250"
```
Note: The oldest version (100) was automatically removed.

### Example 2: Time-Based Retention
```
redis> CONFIG RETENTION LAST 60s
OK - Retention policy set to LAST 60s (keep versions from last 60 seconds)

redis> SET status active
OK

# Wait 65 seconds...

redis> SET status inactive
OK

redis> HISTORY status
1 version(s):
1) [2026-02-02 03:26:30.000] "inactive"
```
Note: The version older than 60 seconds was removed.

### Example 3: Dynamic Policy Changes
```
redis> SET value v1
OK
redis> SET value v2
OK
redis> SET value v3
OK
redis> SET value v4
OK
redis> SET value v5
OK

redis> HISTORY value
5 version(s):
...all 5 versions shown...

redis> CONFIG RETENTION LAST 2
OK - Retention policy set to LAST 2 (keep last 2 versions)

redis> HISTORY value
2 version(s):
1) [2026-02-02 03:25:24.134] "v4"
2) [2026-02-02 03:25:24.135] "v5"
```
Note: Changing the policy immediately prunes all existing keys.

## Behavior Details

### When Retention is Applied
1. **After each SET**: When a new version is added
2. **During WAL replay**: When versions are restored from disk
3. **On policy change**: Immediately when CONFIG RETENTION is executed

### Effect on Commands

**GET key**
- Always returns the latest version (unaffected by retention)

**GET key AT timestamp**
- Returns the version at or before the timestamp
- If the version was pruned by retention, returns (nil) or the next available version

**HISTORY key**
- Shows only the versions that survived retention
- Older pruned versions are not visible

**DEL key**
- Deletes all versions (including those kept by retention)

### Multiple Keys
The retention policy applies **globally** to all keys. Each key maintains its own version history subject to the configured policy.

```
redis> CONFIG RETENTION LAST 2
OK - Retention policy set to LAST 2 (keep last 2 versions)

redis> SET key1 a
redis> SET key1 b
redis> SET key1 c

redis> SET key2 x
redis> SET key2 y
redis> SET key2 z

# Both keys keep only 2 versions
redis> HISTORY key1
2 version(s): b, c

redis> HISTORY key2
2 version(s): y, z
```

## Important Notes

### Persistence
⚠️ **Retention policy configuration is NOT persisted across restarts.**

- After restart, the policy defaults to FULL
- All versions in the WAL are replayed
- You must re-configure retention after each restart

Example:
```bash
# Session 1
redis> CONFIG RETENTION LAST 2
OK - Retention policy set to LAST 2 (keep last 2 versions)
redis> SET value v1
redis> SET value v2
redis> SET value v3
redis> EXIT

# Session 2 (after restart)
redis> HISTORY value
3 version(s)  # All versions restored!
redis> CONFIG RETENTION LAST 2  # Must re-apply
OK - Retention policy set to LAST 2 (keep last 2 versions)
redis> HISTORY value
2 version(s)  # Now pruned
```

### WAL Behavior
- WAL logs ALL SET operations regardless of retention
- On replay, ALL versions are restored
- Then retention is applied (if configured)
- This ensures you don't lose data in the WAL

### Snapshots
- Snapshots save only the **latest version** of each key
- Historical versions are lost in snapshots (regardless of retention policy)
- This is by design for compaction

## Error Handling

```
# Missing parameters
redis> CONFIG RETENTION LAST
(error) ERR LAST requires a value parameter
Usage: CONFIG RETENTION LAST <N> for count, or CONFIG RETENTION LAST <T>s for time

# Invalid values
redis> CONFIG RETENTION LAST 0
(error) ERR count must be positive

redis> CONFIG RETENTION LAST abc
(error) ERR invalid count value

redis> CONFIG RETENTION LAST 10x
(error) ERR invalid count value

redis> CONFIG RETENTION LAST s
(error) ERR invalid format, expected number before 's'

# Unknown mode
redis> CONFIG RETENTION KEEP_ALL
(error) ERR unknown retention mode 'KEEP_ALL'
Valid modes: FULL, LAST <N>, LAST <T>s
```

## Use Cases

1. **Production Monitoring**: Keep only recent metrics
   ```
   CONFIG RETENTION LAST 86400s  # Last 24 hours
   ```

2. **Change Tracking**: Limit audit trail size
   ```
   CONFIG RETENTION LAST 100  # Last 100 changes
   ```

3. **Full Audit**: Keep complete history
   ```
   CONFIG RETENTION FULL
   ```

4. **Memory Management**: Prevent unbounded growth
   ```
   CONFIG RETENTION LAST 10
   ```

## Performance Impact

- **Memory**: Reduced by limiting versions
- **SET Performance**: Minimal overhead (prunes old versions)
- **GET Performance**: No impact (uses latest version)
- **HISTORY Performance**: Faster (fewer versions to return)
- **GET AT Performance**: May fail if queried version was pruned

## Command Reference

```
CONFIG RETENTION FULL
CONFIG RETENTION LAST <count>
CONFIG RETENTION LAST <seconds>s
```

Parameters:
- `<count>`: Integer > 0 (number of versions to keep)
- `<seconds>`: Integer > 0 (time window in seconds, must end with 's')

Examples:
```
CONFIG RETENTION FULL          # Keep all versions
CONFIG RETENTION LAST 5        # Keep last 5 versions
CONFIG RETENTION LAST 60s      # Keep versions from last 60 seconds
CONFIG RETENTION LAST 3600s    # Keep versions from last hour
```
