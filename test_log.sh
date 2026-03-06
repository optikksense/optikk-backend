#!/bin/bash

# Comprehensive test for OTLP logs ingestion via HTTP

API_KEY="298acd2c840247ecd956fc294f81a55a775a39f8f9efbebc219f11a03997cbf2"

echo "=========================================="
echo "OTLP Logs Ingestion Test"
echo "=========================================="
echo ""

# Get initial count
INITIAL_COUNT=$(docker exec -it clickhouse clickhouse-client --query "SELECT count(*) FROM observability.logs" | tr -d '\r')
echo "Initial logs count: ${INITIAL_COUNT}"
echo ""

# Test 1: HTTP/JSON with INFO log
echo "Test 1: Sending INFO log via HTTP/JSON..."
TIMESTAMP_1=$(python3 -c "import time; print(int(time.time() * 1_000_000_000))")
curl -s -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d "{
    \"resourceLogs\": [{
      \"resource\": {
        \"attributes\": [
          { \"key\": \"service.name\", \"value\": { \"stringValue\": \"test-log-service\" } },
          { \"key\": \"deployment.environment\", \"value\": { \"stringValue\": \"staging\" } }
        ]
      },
      \"scopeLogs\": [{
        \"scope\": { \"name\": \"test.scope\", \"version\": \"1.0.0\" },
        \"logRecords\": [{
          \"timeUnixNano\": \"${TIMESTAMP_1}\",
          \"observedTimeUnixNano\": \"${TIMESTAMP_1}\",
          \"severityNumber\": 9,
          \"severityText\": \"INFO\",
          \"body\": { \"stringValue\": \"This is a test INFO log message from test_log.sh\" },
          \"attributes\": [
            { \"key\": \"http.method\", \"value\": { \"stringValue\": \"GET\" } },
            { \"key\": \"http.status_code\", \"value\": { \"intValue\": 200 } }
          ],
          \"traceId\": \"5b8aa5a2d2c864ce9371f2aa74aee2b7\",
          \"spanId\": \"5a8cb5d6e2a864ce\"
        }]
      }]
    }]
  }" > /dev/null
echo "✓ Sent"

# Test 2: HTTP/JSON with ERROR log
echo "Test 2: Sending ERROR log via HTTP/JSON..."
TIMESTAMP_2=$(python3 -c "import time; print(int(time.time() * 1_000_000_000))")
curl -s -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d "{
    \"resourceLogs\": [{
      \"resource\": {
        \"attributes\": [
          { \"key\": \"service.name\", \"value\": { \"stringValue\": \"test-log-service\" } }
        ]
      },
      \"scopeLogs\": [{
        \"scope\": { \"name\": \"test.scope\", \"version\": \"1.0.0\" },
        \"logRecords\": [{
          \"timeUnixNano\": \"${TIMESTAMP_2}\",
          \"observedTimeUnixNano\": \"${TIMESTAMP_2}\",
          \"severityNumber\": 17,
          \"severityText\": \"ERROR\",
          \"body\": { \"stringValue\": \"This is a test ERROR log message from test_log.sh\" },
          \"attributes\": [
            { \"key\": \"error\", \"value\": { \"boolValue\": true } }
          ]
        }]
      }]
    }]
  }" > /dev/null
echo "✓ Sent"

echo ""
echo "Waiting 3 seconds for async inserts to complete..."
sleep 3

# Verify results
echo ""
echo "=========================================="
echo "Verification Results"
echo "=========================================="
FINAL_COUNT=$(docker exec -it clickhouse clickhouse-client --query "SELECT count(*) FROM observability.logs" | tr -d '\r')
NEW_LOGS=$((FINAL_COUNT - INITIAL_COUNT))

echo "Initial count: ${INITIAL_COUNT}"
echo "Final count:   ${FINAL_COUNT}"
echo "New logs:      ${NEW_LOGS}"
echo ""

if [ "$NEW_LOGS" -ge 2 ]; then
    echo "✅ SUCCESS: At least 2 new logs were inserted!"
else
    echo "❌ FAILURE: Expected at least 2 new logs, got ${NEW_LOGS}"
fi

echo ""
echo "Latest logs:"
docker exec -it clickhouse clickhouse-client --query "
  SELECT 
    timestamp,
    severity_text,
    body,
    service,
    trace_id
  FROM observability.logs 
  ORDER BY timestamp DESC 
  LIMIT 2 
  FORMAT Pretty
"
