#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT=8897
WAL_PATH="data/test_http_json_wal.log"
SNAPSHOT_PATH="data/snapshot.db"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID"
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

start_server() {
    ./build/http_server --port "$PORT" --wal "$WAL_PATH" >/tmp/sdb_http_json.log 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if curl -sS "http://localhost:$PORT/health" >/dev/null 2>&1; then
            return
        fi
        sleep 0.1
    done

    echo "HTTP server failed to become ready"
    cat /tmp/sdb_http_json.log
    exit 1
}

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID"
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

request() {
    local method="$1"
    local path="$2"
    local body="${3-}"

    if [[ -n "$body" ]]; then
        curl -sS -X "$method" "http://localhost:$PORT$path" \
            -H "Content-Type: application/json" \
            -d "$body" \
            -w $'\nHTTP_STATUS:%{http_code}'
    else
        curl -sS -X "$method" "http://localhost:$PORT$path" \
            -w $'\nHTTP_STATUS:%{http_code}'
    fi
}

assert_status() {
    local response="$1"
    local expected="$2"
    local actual="${response##*HTTP_STATUS:}"
    if [[ "$actual" != "$expected" ]]; then
        echo "Expected HTTP $expected, got $actual"
        echo "$response"
        exit 1
    fi
}

assert_contains() {
    local response="$1"
    local expected="$2"
    if [[ "$response" != *"$expected"* ]]; then
        echo "Expected response to contain: $expected"
        echo "$response"
        exit 1
    fi
}

rm -f "$WAL_PATH" "$SNAPSHOT_PATH"
start_server

echo "Test 1: /set accepts spaces and escaped quotes"
response="$(request POST /set '{"key":"message","value":"hello world with \"quotes\" and spaces"}')"
assert_status "$response" 200
assert_contains "$response" '"status":"ok"'

echo "Test 2: /get returns the exact string value"
response="$(request GET '/get?key=message')"
assert_status "$response" 200
assert_contains "$response" 'hello world with'
assert_contains "$response" '\"quotes\" and spaces'

echo "Test 3: /set accepts nested JSON values"
response="$(request POST /set '{"key":"profile","value":{"name":"Alice Smith","quote":"she said \"hi\"","tags":["x y","z"]}}')"
assert_status "$response" 200
assert_contains "$response" '"status":"ok"'

echo "Test 4: /get returns the nested JSON payload stringified"
response="$(request GET '/get?key=profile')"
assert_status "$response" 200
assert_contains "$response" 'Alice Smith'
assert_contains "$response" 'she said'
assert_contains "$response" 'x y'

echo "Test 5: /propose accepts nested JSON values"
response="$(request POST /propose '{"key":"proposal","value":{"team":"blue team","enabled":true}}')"
assert_status "$response" 200
assert_contains "$response" '"result":"ACCEPT"'
assert_contains "$response" 'blue team'

echo "Test 6: /guards accepts a proper JSON array for ENUM values"
response="$(request POST /guards '{"type":"ENUM","name":"role_guard","keyPattern":"role*","values":["admin","editor"]}')"
assert_status "$response" 200
assert_contains "$response" '"status":"ok"'

echo "Test 7: /config/retention accepts a JSON string with spaces"
response="$(request POST /config/retention '{"mode":"LAST 5"}')"
assert_status "$response" 200
assert_contains "$response" '"status":"ok"'

echo "Test 8: /policy accepts valid JSON"
response="$(request POST /policy '{"policy":"STRICT"}')"
assert_status "$response" 200
assert_contains "$response" '"activePolicy":"STRICT"'

echo "Test 9: malformed JSON is rejected"
response="$(request POST /set '{"key":"broken","value":}')"
assert_status "$response" 400
assert_contains "$response" 'Invalid request'

echo "Test 10: values with spaces and nested JSON survive restart"
stop_server
start_server

response="$(request GET '/get?key=message')"
assert_status "$response" 200
assert_contains "$response" 'hello world with'
assert_contains "$response" '\"quotes\" and spaces'

response="$(request GET '/get?key=profile')"
assert_status "$response" 200
assert_contains "$response" 'Alice Smith'
assert_contains "$response" 'x y'

echo "All HTTP JSON parsing tests passed"
