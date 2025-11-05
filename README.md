# <img width="50" height="100" alt="Copy of Untitled Design" src="https://github.com/user-attachments/assets/3693a100-a730-443c-ab6e-dbfabade7867" /> NEUROINDEX

## Operational Overview
You’ll start the RESP server (port 6381), import 1M records via the Python client, create a snapshot from the CLI, then bring up the HTTP server (port 8080) pointing to the same persistence directory. Here’s the step-by-step.

## 1. Start NeuroIndex RESP Server
In a first terminal start the server:
```
./target/release/neuroindex-resp-server --port 6381 --shards 16 --capacity 65536 --log-level info --persistence-dir ./data
```

Verify the port is listening (optional):
```
lsof -i :6381 or ss -lntp | grep 6381
```

## 2. Load 1M records via Python client
In a second terminal run the batch import (5k per batch):
```
python3 clients/python/insert_1m_resp.py --host 0.0.0.0 --port 6381 --total 1000000 --batch-size 5000 --prefix account
```
Tips:
If you hit timeouts, lower --batch-size (e.g., 2000).
Watch the server terminal for backpressure or memory warnings.

## 3. Take a snapshot with neuroindex-cli
Start the CLI:
```
./target/release/neuroindex-cli --host 0.0.0.0 --port 6381
```
From the CLI prompt, run:
```
snapshot
```

Notes:
Wait for the completion message before proceeding.
The snapshot is written under ./data (as configured by the RESP server).
If writes are ongoing, the snapshot reflects the state at the command time

## 4. Start NeuroIndex HTTP Server
In a third terminal:

Launch the process:
```
./target/release/neuroindex-http --port 8080 --shards 16 --capacity 65536 --log-level info --persistence-dir ./data
```

Verify the port is up:
```
lsof -i :8080 or curl http://127.0.0.1:8080/health (if a health endpoint is available)
```

## CHECK FOR MEMORY USAGE
```
ps -C neuroindex-http -o rss= | awk '{sum+=$1} END {printf "RAM: %.2f MB\n", sum/1024}'
```

## QUERY REST API FOR TEST

### Health check
```
curl -s http://127.0.0.1:8080/api/v1/health | jq
```
### Stats
```
curl -s http://127.0.0.1:8080/api/v1/stats | jq
```

### GET singolo
```
curl -s http://127.0.0.1:8080/api/v1/records/account:0050000 | jq
```

### Range query
```
curl -s "http://127.0.0.1:8080/api/v1/records/range?start=account:0000000&end=account:0100000&limit=50" | jq
```
```
curl -s "http://172.16.99.30:8080/api/v1/records/range?start=account:0900000&end=account:0910000&ts=_701" | jq #range query where ts key contain 701
```

### Count aggregation
```
curl -s "http://127.0.0.1:8080/api/v1/aggregations/count?start=account:0000000&end=account:0100000" | jq
```

### Bulk insert
```
curl -X POST http://127.0.0.1:8080/api/v1/records/bulk -H "Content-Type: application/json" -d '{"records":[{"key":"demo:001","value":{"name":"Test User 1","active":true}},{"key":"demo:002","value":{"name":"Test User 2","active":false}}]}' | jq
```

## PARAMETERS REST API
| Command   | Description |
|---------|-------------|
| **start** | start key |
| **end** | end key |
| **limit** | max result number of elements |

## CLI COMMANDS
| Command   | Description | Example |
|---------|-------------|---------|
| **PING** | Test connection | `PING` → `PONG` |
| **ECHO** | Echo message | `ECHO "hello"` → `"hello"` |
| **SET** | Set key-value | `SET key value` → `OK` |
| **GET** | Get value | `GET key` → `value` |
| **MSET** | Set multiple | `MSET k1 v1 k2 v2` → `OK` |
| **MGET** | Get multiple | `MGET k1 k2` → `[v1, v2]` |
| **DEL** | Delete keys | `DEL k1 k2` → `2` (count) |
| **EXISTS** | Check existence | `EXISTS k1 k2` → `2` (count) |
| **KEYS** | List all keys | `KEYS *` → `[k1, k2, ...]` |
| **DBSIZE** | Database size | `DBSIZE` → `100` |
| **INFO** | Server info | `INFO` → `"...stats..."` |
| **QUIT** | Close connection | `QUIT` → `OK` |


