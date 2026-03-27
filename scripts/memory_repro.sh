#!/bin/bash
# Memory reproduction test for KalamDB
# Inserts 100K rows and monitors memory at each stage
set -e

SERVER="http://localhost:8080"
API="$SERVER/v1/api"

# Get server PID
PID=$(lsof -i :8080 -t 2>/dev/null | head -1)
if [ -z "$PID" ]; then
    echo "ERROR: No KalamDB server running on port 8080"
    exit 1
fi
echo "Server PID: $PID"

get_mem() {
    # macOS: RSS in KB from ps
    local rss=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
    local mb=$((rss / 1024))
    echo "$mb"
}

log_mem() {
    local label="$1"
    local mb=$(get_mem)
    echo "[MEM] $label: ${mb} MB (RSS)"
}

# Step 0: Login
echo "=== Step 0: Authenticate ==="
LOGIN_RESP=$(curl -s "$API/auth/login" -H "Content-Type: application/json" -d '{"username":"root","password":"kalamdb123"}')
TOKEN=$(echo "$LOGIN_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))" 2>/dev/null)
if [ -z "$TOKEN" ]; then
    echo "Login failed: $LOGIN_RESP"
    echo "Trying setup first..."
    SETUP_RESP=$(curl -s "$API/auth/setup" -H "Content-Type: application/json" -d '{"username":"admin","root_password":"kalamdb123","password":"kalamdb123"}')
    echo "Setup: $SETUP_RESP"
    LOGIN_RESP=$(curl -s "$API/auth/login" -H "Content-Type: application/json" -d '{"username":"root","password":"kalamdb123"}')
    TOKEN=$(echo "$LOGIN_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))" 2>/dev/null)
fi

if [ -z "$TOKEN" ]; then
    echo "FATAL: Could not authenticate. Response: $LOGIN_RESP"
    exit 1
fi
echo "Authenticated (token length: ${#TOKEN})"

AUTH="Authorization: Bearer $TOKEN"

sql() {
    curl -s "$API/sql" -H "$AUTH" -H "Content-Type: application/json" -d "{\"sql\": \"$1\"}"
}

# Step 1: Baseline memory
echo ""
echo "=== Step 1: Baseline ==="
log_mem "Baseline (before any operations)"

# Step 2: Create test namespace + table
echo ""
echo "=== Step 2: Create test table ==="
sql "CREATE NAMESPACE IF NOT EXISTS memtest" > /dev/null
sql "CREATE TABLE IF NOT EXISTS memtest.bulk (id TEXT PRIMARY KEY, value TEXT, num INT, data TEXT)" > /dev/null
echo "Table created"
log_mem "After CREATE TABLE"

# Step 3: Insert 100K rows in batches of 500
echo ""
echo "=== Step 3: Insert 100K rows (batches of 500) ==="
TOTAL=100000
BATCH=500
INSERTED=0

log_mem "Before inserts"

while [ $INSERTED -lt $TOTAL ]; do
    # Build batch INSERT
    VALUES=""
    for i in $(seq 1 $BATCH); do
        ROW_NUM=$((INSERTED + i))
        if [ $ROW_NUM -gt $TOTAL ]; then
            break
        fi
        if [ -n "$VALUES" ]; then
            VALUES="$VALUES,"
        fi
        # Generate a reasonably sized row (~200 bytes of data per row)
        VALUES="$VALUES('row-$ROW_NUM', 'value-$ROW_NUM-payload-data-here', $ROW_NUM, 'Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua row $ROW_NUM')"
    done

    RESP=$(sql "INSERT INTO memtest.bulk (id, value, num, data) VALUES $VALUES")
    INSERTED=$((INSERTED + BATCH))
    
    # Log memory every 10K rows
    if [ $((INSERTED % 10000)) -eq 0 ]; then
        log_mem "After ${INSERTED} rows inserted"
    fi
done

echo "Inserted $TOTAL rows total"
log_mem "After all inserts"

# Step 4: Wait and check memory
echo ""
echo "=== Step 4: Wait 10s for any background processing ==="
sleep 10
log_mem "After 10s idle"

# Step 5: Query the table (DataFusion scan)
echo ""
echo "=== Step 5: Query (SELECT COUNT) ==="
RESP=$(sql "SELECT COUNT(*) FROM memtest.bulk")
echo "Query result: $RESP"
log_mem "After SELECT COUNT"

# Step 6: Trigger flush by checking stats (the flush check interval is 60s)
echo ""
echo "=== Step 6: Wait for flush (up to 120s) ==="
log_mem "Before flush wait"
for i in $(seq 1 12); do
    sleep 10
    log_mem "Waiting for flush (${i}0s elapsed)"
    # Check if parquet files appeared
    PARQUET_COUNT=$(find /Users/jamal/git/KalamDB/backend/data/storage/memtest/bulk/ -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$PARQUET_COUNT" -gt 0 ]; then
        echo "Flush completed! $PARQUET_COUNT parquet files"
        break
    fi
done

log_mem "After flush"

# Step 7: Wait more to see if memory goes back down
echo ""
echo "=== Step 7: Post-flush idle monitoring (60s) ==="
for i in $(seq 1 6); do
    sleep 10
    log_mem "Post-flush idle (${i}0s)"
done

# Step 8: Run some queries
echo ""
echo "=== Step 8: Run queries after flush ==="
for i in $(seq 1 5); do
    sql "SELECT COUNT(*) FROM memtest.bulk" > /dev/null
    sql "SELECT * FROM memtest.bulk WHERE num < 100 LIMIT 10" > /dev/null
done
log_mem "After queries"

# Step 9: Insert another batch (10K) to test steady-state
echo ""
echo "=== Step 9: Insert 10K more rows ==="
for batch_start in $(seq 100001 500 110000); do
    VALUES=""
    for i in $(seq 0 499); do
        ROW_NUM=$((batch_start + i))
        if [ -n "$VALUES" ]; then
            VALUES="$VALUES,"
        fi
        VALUES="$VALUES('row-$ROW_NUM', 'value-$ROW_NUM', $ROW_NUM, 'more data for row $ROW_NUM to test steady state memory')"
    done
    sql "INSERT INTO memtest.bulk (id, value, num, data) VALUES $VALUES" > /dev/null
done
log_mem "After 10K more rows"

# Final summary
echo ""
echo "=== Summary ==="
log_mem "Final memory"
echo "Done. Check the memory progression above for anomalies."
