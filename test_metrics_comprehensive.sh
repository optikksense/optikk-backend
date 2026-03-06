#!/bin/bash

# Comprehensive test for OTLP metrics ingestion via HTTP and gRPC

API_KEY="298acd2c840247ecd956fc294f81a55a775a39f8f9efbebc219f11a03997cbf2"

echo "=========================================="
echo "OTLP Metrics Ingestion Test"
echo "=========================================="
echo ""

# Get initial count
INITIAL_COUNT=$(docker exec -it clickhouse clickhouse-client --query "SELECT count(*) FROM observability.metrics" | tr -d '\r')
echo "Initial metrics count: ${INITIAL_COUNT}"
echo ""

# Test 1: HTTP/JSON with Gauge
echo "Test 1: Sending Gauge metric via HTTP/JSON..."
TIMESTAMP_1=$(python3 -c "import time; print(int(time.time() * 1_000_000_000))")
curl -s -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d "{
    \"resourceMetrics\": [{
      \"resource\": {
        \"attributes\": [
          { \"key\": \"service.name\", \"value\": { \"stringValue\": \"test-http-service\" } },
          { \"key\": \"deployment.environment\", \"value\": { \"stringValue\": \"staging\" } }
        ]
      },
      \"scopeMetrics\": [{
        \"scope\": { \"name\": \"test.scope\", \"version\": \"1.0.0\" },
        \"metrics\": [{
          \"name\": \"http.test.gauge\",
          \"unit\": \"bytes\",
          \"description\": \"Test gauge metric\",
          \"gauge\": {
            \"dataPoints\": [
              { \"timeUnixNano\": \"${TIMESTAMP_1}\", \"asDouble\": 1024.5 }
            ]
          }
        }]
      }]
    }]
  }" > /dev/null
echo "✓ Sent"

# Test 2: HTTP/JSON with Sum
echo "Test 2: Sending Sum metric via HTTP/JSON..."
TIMESTAMP_2=$(python3 -c "import time; print(int(time.time() * 1_000_000_000))")
curl -s -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d "{
    \"resourceMetrics\": [{
      \"resource\": {
        \"attributes\": [
          { \"key\": \"service.name\", \"value\": { \"stringValue\": \"test-http-service\" } }
        ]
      },
      \"scopeMetrics\": [{
        \"scope\": { \"name\": \"test.scope\", \"version\": \"1.0.0\" },
        \"metrics\": [{
          \"name\": \"http.test.counter\",
          \"unit\": \"1\",
          \"sum\": {
            \"aggregationTemporality\": \"AGGREGATION_TEMPORALITY_CUMULATIVE\",
            \"isMonotonic\": true,
            \"dataPoints\": [
              { \"timeUnixNano\": \"${TIMESTAMP_2}\", \"asInt\": \"42\" }
            ]
          }
        }]
      }]
    }]
  }" > /dev/null
echo "✓ Sent"

# Test 3: HTTP/JSON with Histogram
echo "Test 3: Sending Histogram metric via HTTP/JSON..."
TIMESTAMP_3=$(python3 -c "import time; print(int(time.time() * 1_000_000_000))")
curl -s -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -H "x-api-key: ${API_KEY}" \
  -d "{
    \"resourceMetrics\": [{
      \"resource\": {
        \"attributes\": [
          { \"key\": \"service.name\", \"value\": { \"stringValue\": \"test-http-service\" } }
        ]
      },
      \"scopeMetrics\": [{
        \"scope\": { \"name\": \"test.scope\", \"version\": \"1.0.0\" },
        \"metrics\": [{
          \"name\": \"http.test.histogram\",
          \"unit\": \"ms\",
          \"histogram\": {
            \"aggregationTemporality\": \"AGGREGATION_TEMPORALITY_DELTA\",
            \"dataPoints\": [{
              \"timeUnixNano\": \"${TIMESTAMP_3}\",
              \"count\": 100,
              \"sum\": 5000.0,
              \"explicitBounds\": [10, 50, 100, 500, 1000],
              \"bucketCounts\": [5, 20, 30, 25, 15, 5]
            }]
          }
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
FINAL_COUNT=$(docker exec -it clickhouse clickhouse-client --query "SELECT count(*) FROM observability.metrics" | tr -d '\r')
NEW_METRICS=$((FINAL_COUNT - INITIAL_COUNT))

echo "Initial count: ${INITIAL_COUNT}"
echo "Final count:   ${FINAL_COUNT}"
echo "New metrics:   ${NEW_METRICS}"
echo ""

if [ "$NEW_METRICS" -ge 3 ]; then
    echo "✅ SUCCESS: At least 3 new metrics were inserted!"
else
    echo "❌ FAILURE: Expected at least 3 new metrics, got ${NEW_METRICS}"
fi

echo ""
echo "Latest metrics:"
docker exec -it clickhouse clickhouse-client --query "
  SELECT 
    metric_name, 
    metric_type, 
    value, 
    service,
    environment,
    timestamp 
  FROM observability.metrics 
  ORDER BY timestamp DESC 
  LIMIT 5 
  FORMAT Pretty
"

