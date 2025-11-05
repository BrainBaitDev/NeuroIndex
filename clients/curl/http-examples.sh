#!/bin/bash
#
# NeuroIndex HTTP REST API Examples
# Using curl to interact with the HTTP server
#

HOST="localhost"
PORT="8080"
BASE_URL="http://${HOST}:${PORT}/api/v1"

echo "========================================"
echo "NeuroIndex HTTP REST API Examples"
echo "========================================"
echo ""

echo "1. Health Check"
echo "----------------"
curl -s "${BASE_URL}/health" | jq .
echo ""

echo "2. Server Stats"
echo "----------------"
curl -s "${BASE_URL}/stats" | jq .
echo ""

echo "3. PUT Record (user:1)"
echo "----------------"
curl -s -X POST "${BASE_URL}/records/user:1" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Alice", "age": 30, "email": "alice@example.com"}}' | jq .
echo ""

echo "4. GET Record (user:1)"
echo "----------------"
curl -s "${BASE_URL}/records/user:1" | jq .
echo ""

echo "5. Bulk Insert (10 records)"
echo "----------------"
curl -s -X POST "${BASE_URL}/records/bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {"key": "user:2", "value": {"name": "Bob", "score": 100}},
      {"key": "user:3", "value": {"name": "Charlie", "score": 200}},
      {"key": "user:4", "value": {"name": "David", "score": 300}},
      {"key": "user:5", "value": {"name": "Eve", "score": 400}},
      {"key": "user:6", "value": {"name": "Frank", "score": 500}},
      {"key": "user:7", "value": {"name": "Grace", "score": 600}},
      {"key": "user:8", "value": {"name": "Henry", "score": 700}},
      {"key": "user:9", "value": {"name": "Iris", "score": 800}},
      {"key": "user:10", "value": {"name": "Jack", "score": 900}},
      {"key": "user:11", "value": {"name": "Kate", "score": 1000}}
    ]
  }' | jq .
echo ""

echo "6. Range Query (user:2 to user:5)"
echo "----------------"
curl -s "${BASE_URL}/records/range?start=user:2&end=user:5&limit=10" | jq .
echo ""

echo "7. Count Aggregation (user:1 to user:9)"
echo "----------------"
curl -s "${BASE_URL}/aggregations/count?start=user:1&end=user:9" | jq .
echo ""

echo "8. DELETE Record (user:1)"
echo "----------------"
curl -s -X DELETE "${BASE_URL}/records/user:1"
echo "✓ Deleted (HTTP 204)"
echo ""

echo "9. Verify deletion (should return 404)"
echo "----------------"
curl -s -w "HTTP Status: %{http_code}\n" "${BASE_URL}/records/user:1"
echo ""

echo "10. Final Stats"
echo "----------------"
curl -s "${BASE_URL}/stats" | jq .
echo ""

echo "========================================"
echo "✓ Examples completed!"
echo "========================================"
echo ""
echo "Try bulk inserting 10,000 records:"
echo "  python3 ../python/insert_10k_http.py"
echo ""
echo "Or with jq and curl:"
echo '  python3 -c "import json; print(json.dumps({\"records\": [{\"key\": f\"user:{i:05d}\", \"value\": {\"name\": f\"User {i}\"}} for i in range(10000)]}))" | curl -X POST -H "Content-Type: application/json" -d @- '${BASE_URL}'/records/bulk'
