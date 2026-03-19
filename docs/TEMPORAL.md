# Temporal Versioning Features

This Redis-like key-value database supports **temporal versioning**, allowing you to track the complete history of values for each key and query them at any point in time.

## Overview

Each key maintains a chronological list of versions. Every time you `SET` a key, a new version is appended with the current timestamp, rather than overwriting the previous value.

## Data Structure

Each version contains:
- **Timestamp**: `std::chrono::system_clock::time_point` - When the value was set
- **Value**: `std::string` - The actual data

## User Commands

### Basic Operations

#### `SET key value`
Appends a new version with the current timestamp. Does not overwrite previous versions.

```
redis> SET price 100
OK
redis> SET price 150
OK
redis> SET price 200
OK
```

#### `GET key`
Returns the latest (most recent) version of the value.

```
redis> GET price
"200"
```

#### `DEL key`
Removes all versions of the key.

```
redis> DEL price
(integer) 1
```

### Temporal Query Commands

#### `HISTORY key`
Shows all versions of a key with their timestamps in chronological order.

```
redis> HISTORY price
3 version(s):
1) [2026-02-02 02:52:19.567] "100"
2) [2026-02-02 02:52:19.568] "150"
3) [2026-02-02 02:52:19.569] "200"
```

Returns `(empty array)` if the key doesn't exist.

#### `GET key AT <timestamp>`
Returns the value that was active at or before the specified timestamp. Supports two formats:

**Epoch milliseconds:**
```
redis> GET price AT 1738467139567
"100"
```

**ISO-8601 format:**
```
redis> GET price AT 2026-02-02 02:52:19
"150"
```

Returns `(nil)` if:
- The key doesn't exist
- No version exists at or before the given timestamp
- Invalid timestamp format

**Timestamp Format Requirements:**
- Epoch milliseconds: Positive integer (e.g., `1738467139567`)
- ISO-8601: `YYYY-MM-DD HH:MM:SS` (e.g., `2026-02-02 14:30:00`)

## Programmatic API

### C++ API Reference

#### `set(key, value)`
Appends a new version with the current timestamp.

```cpp
kvstore->set("price", "100");  // Version 1
kvstore->set("price", "150");  // Version 2
kvstore->set("price", "200");  // Version 3
```

#### `get(key)`
Returns the latest version of the value.

```cpp
auto value = kvstore->get("price");  // Returns "200"
```

#### `getAtTime(key, timestamp)`
Returns the value that was active at or before the specified timestamp.

```cpp
auto time1 = std::chrono::system_clock::now();
kvstore->set("status", "pending");

std::this_thread::sleep_for(std::chrono::seconds(1));
auto time2 = std::chrono::system_clock::now();
kvstore->set("status", "completed");

// Query historical state
auto oldValue = kvstore->getAtTime("status", time1);  // Returns "pending"
auto newValue = kvstore->getAtTime("status", time2);  // Returns "completed"
```

Returns `std::nullopt` if:
- The key doesn't exist
- No version exists at or before the given timestamp

#### `getHistory(key)`
Returns all versions of a key in chronological order.

```cpp
std::vector<Version> history = kvstore->getHistory("price");

for (const auto& version : history) {
    std::cout << "At " << formatTime(version.timestamp) 
              << " value was: " << version.value << "\n";
}
```

Returns an empty vector if the key doesn't exist.

## Example Usage

See [test_temporal.cpp](../src/test_temporal.cpp) for a complete example demonstrating:
- Multiple versions per key
- Time-travel queries
- Historical value retrieval
- Multiple versioned keys

Run the test:
```bash
cd build
./test_temporal
```

## Persistence Behavior

### Snapshots
- Snapshots save **only the latest version** of each key
- This keeps snapshot files compact
- Historical versions before the snapshot are lost
- Snapshot format: `SET key value` (no timestamp)

### WAL (Write-Ahead Log)
- Each `SET` operation is logged with its timestamp: `SET key value timestamp_ms`
- Timestamps are stored as epoch milliseconds for precision
- On replay, each logged SET restores the version with its **original timestamp**
- Timestamps are preserved across restarts
- This enables true temporal persistence

**WAL Format Example:**
```
SET price 100 1769980227253
SET price 150 1769980227304
SET price 200 1769980227355
```

### Recovery Process
1. Load snapshot (if exists) - creates versions with current timestamp
2. Replay WAL with original timestamps - preserves exact history
3. Result: Complete temporal history is restored

### Recommendations
- Use `SNAPSHOT` periodically to compact storage
- Historical queries work perfectly across restarts
- WAL maintains full version history with precise timestamps

## Use Cases

1. **Price History**: Track how prices changed over time
2. **Configuration Changes**: See what values were active at any point
3.Add version limits per key (LRU eviction)
- Implement version expiration/TTL
- Add range queries (get all changes between two timestamps)
- Optimize getAtTime with binary search
- Store timestamps in snapshots for complete history preservation

## Performance Considerations

- Each `SET` appends a version (memory grows with updates)
- `GET` is O(1) - returns last element
- `HISTORY` is O(n) where n = number of versions
- `GET AT` is O(n) where n = number of versions
- `getHistory` is O(n) to copy versions
- Consider using `SNAPSHOT` to reset history and free memory

## Future Enhancements

Possible improvements:
- Add version limits per key (LRU eviction)
- Implement version expiration/TTL
- Add range queries (get all changes between two timestamps)
- Optimize getAtTime with binary search
- Store timestamps in snapshots for complete history preservation
