# HTTP API Documentation

## Overview
The Temporal Database HTTP Server provides a REST API for interacting with the temporal key-value store. All endpoints return JSON responses.

## Server Configuration

### Starting the Server
```bash
./http_server [OPTIONS]
```

**Options:**
- `--port <num>` - HTTP port (default: 8080)
- `--wal <path>` - WAL file path (default: no WAL)
- `--help` - Show help message

**Examples:**
```bash
# Start on default port 8080
./http_server

# Start on custom port with WAL enabled
./http_server --port 9000 --wal database.wal
```

## API Endpoints

### Health Check
**GET** `/health`

Check server status.

**Response:**
```json
{
  "status": "ok"
}
```

**Example:**
```bash
curl http://localhost:8080/health
```

---

### Set Key-Value Pair
**POST** `/set`

Store a key-value pair with current timestamp.

**Request Body:**
```json
{
  "key": "string",
  "value": "string"
}
```

**Success Response (200):**
```json
{
  "status": "ok",
  "message": "Key 'mykey' set successfully"
}
```

**Error Response (400/500):**
```json
{
  "error": "Error message"
}
```

**Examples:**
```bash
# Set a key
curl -X POST http://localhost:8080/set \
  -H "Content-Type: application/json" \
  -d '{"key":"price","value":"100"}'

# Set another version
curl -X POST http://localhost:8080/set \
  -H "Content-Type: application/json" \
  -d '{"key":"price","value":"150"}'
```

---

### Get Current Value
**GET** `/get?key=<key>`

Retrieve the current (latest) value for a key.

**Query Parameters:**
- `key` (required) - The key to retrieve

**Success Response (200):**
```json
{
  "key": "mykey",
  "value": "current_value"
}
```

**Not Found Response (404):**
```json
{
  "error": "Key not found",
  "key": "mykey"
}
```

**Examples:**
```bash
# Get current value
curl http://localhost:8080/get?key=price

# Response: {"key":"price","value":"150"}
```

---

### Get Value at Timestamp
**GET** `/getAt?key=<key>&timestamp=<timestamp>`

Retrieve the value of a key at or before a specific timestamp.

**Query Parameters:**
- `key` (required) - The key to retrieve
- `timestamp` (required) - Timestamp in format: `YYYY-MM-DD HH:MM:SS.mmm`

**Success Response (200):**
```json
{
  "key": "mykey",
  "value": "historical_value",
  "timestamp": "2026-02-02 09:17:26.000"
}
```

**Not Found Response (404):**
```json
{
  "error": "No version found at or before timestamp",
  "key": "mykey",
  "timestamp": "2026-02-02 09:17:26.000"
}
```

**Examples:**
```bash
# Get value at specific time
curl "http://localhost:8080/getAt?key=price&timestamp=2026-02-02%2009:17:26.000"

# URL-encoded timestamp (space = %20, colon = %3A can be used)
curl "http://localhost:8080/getAt?key=price&timestamp=2026-02-02+09:17:26.000"
```

---

### Get Version History
**GET** `/history?key=<key>`

Retrieve all versions of a key (subject to retention policy).

**Query Parameters:**
- `key` (required) - The key to retrieve history for

**Success Response (200):**
```json
{
  "key": "mykey",
  "versions": [
    {
      "timestamp": "2026-02-02 09:16:47.303",
      "value": "100"
    },
    {
      "timestamp": "2026-02-02 09:17:26.181",
      "value": "150"
    },
    {
      "timestamp": "2026-02-02 09:17:26.185",
      "value": "200"
    }
  ]
}
```

**Empty History Response (200):**
```json
{
  "key": "mykey",
  "versions": []
}
```

**Examples:**
```bash
# Get all versions
curl http://localhost:8080/history?key=price
```

---

### Configure Retention Policy
**POST** `/config/retention`

Set the temporal retention policy for the database.

**Request Body:**
```json
{
  "mode": "FULL | LAST N | LAST Ts"
}
```

**Retention Modes:**
- `FULL` - Keep all versions (default)
- `LAST N` - Keep only the last N versions (e.g., `LAST 5`)
- `LAST Ts` - Keep versions from the last T seconds (e.g., `LAST 3600s`)

**Success Response (200):**
```json
{
  "status": "ok",
  "message": "Retention policy set to LAST 5 (keep last 5 versions)"
}
```

**Error Response (400):**
```json
{
  "error": "Invalid mode. Use 'FULL', 'LAST N', or 'LAST Ts'"
}
```

**Examples:**
```bash
# Keep all versions
curl -X POST http://localhost:8080/config/retention \
  -H "Content-Type: application/json" \
  -d '{"mode":"FULL"}'

# Keep last 5 versions
curl -X POST http://localhost:8080/config/retention \
  -H "Content-Type: application/json" \
  -d '{"mode":"LAST 5"}'

# Keep versions from last hour
curl -X POST http://localhost:8080/config/retention \
  -H "Content-Type: application/json" \
  -d '{"mode":"LAST 3600s"}'
```

---

## Error Handling

All endpoints use standard HTTP status codes:

- **200 OK** - Request successful
- **400 Bad Request** - Invalid request parameters or JSON
- **404 Not Found** - Key not found
- **500 Internal Server Error** - Server error

Error responses always include an `error` field with a description:
```json
{
  "error": "Description of what went wrong"
}
```

---

## Complete Usage Example

```bash
# Start the server
./http_server --port 8080

# Set some values
curl -X POST http://localhost:8080/set -d '{"key":"temp","value":"20"}'
curl -X POST http://localhost:8080/set -d '{"key":"temp","value":"22"}'
curl -X POST http://localhost:8080/set -d '{"key":"temp","value":"25"}'

# Get current value
curl http://localhost:8080/get?key=temp
# Output: {"key":"temp","value":"25"}

# Get history
curl http://localhost:8080/history?key=temp
# Output: {"key":"temp","versions":[...3 versions...]}

# Set retention to keep only last 2 versions
curl -X POST http://localhost:8080/config/retention -d '{"mode":"LAST 2"}'

# Add another version (triggers retention)
curl -X POST http://localhost:8080/set -d '{"key":"temp","value":"27"}'

# Check history (should only have last 2 versions)
curl http://localhost:8080/history?key=temp
# Output: {"key":"temp","versions":[...2 versions...]}
```

---

## CORS Support

The server includes CORS headers for cross-origin requests:
- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, POST, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type`

This allows the API to be used from web applications hosted on different domains.

---

## Building and Running

### Build
```bash
cmake .
make
```

This creates the `http_server` executable.

### Run
```bash
# Simple start
./http_server

# With options
./http_server --port 9000 --wal mydata.wal
```

### Test
```bash
# Health check
curl http://localhost:8080/health

# Run the example commands above
```

---

## Architecture Notes

- **Thin Server Design**: The HTTP server is a thin wrapper around the existing `KVStore` class
- **No Business Logic**: All database logic remains in `KVStore`; the server only handles HTTP/JSON translation
- **Stateful**: The server maintains one `KVStore` instance in memory
- **Single-threaded**: Uses synchronous request handling
- **Header-only HTTP**: Uses cpp-httplib for minimal dependencies
- **Simple JSON**: Custom lightweight JSON parser for basic key-value pairs

---

## Dependencies

- **cpp-httplib** (header-only, included in `include/external/httplib.h`)
- **pthread** (Linux/Unix systems, for cpp-httplib threading)
- **C++17** standard library

No external JSON library required - uses custom minimal JSON parser for the simple request/response formats needed.
