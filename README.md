<div align="center">
  <img width="496" height="312" alt="logo_neuro_index" src="https://github.com/user-attachments/assets/875d11e0-aa42-4086-8f18-c298eeb0998e" />
</div>

<div align="center">

[🚀 Quick Start](https://github.com/BrainBaitDev/NeuroIndex/blob/main/docs/QUICK_START.md) • [📖 Docs](https://github.com/BrainBaitDev/NeuroIndex/blob/main/docs) 


NeuroIndex is an in-memory database engine written in Rust, designed to deliver extreme performance (<200ns per GET) and multi-platform compatibility (Rust API, REST, Redis protocol, gRPC).

</div>

## Key Features
<table align="center">
    <tr>
        <td align="center"> <b>Engine model:</b><br> Sharded multi-core engine with T-Tree + Cuckoo + Hopscotch</td>
        <td align="center"><b>Threading:</b><br> Native parallelism with lock-free shards that scales across all cores without the need for multiple clusters</td>
    </tr>
    <tr>
        <td align="center"><b>Data Model and Query Layer:</b><br> Native support for JSON and an SQL-like engine that allows aggregations, range queries, and joins directly on the engine</td>
        <td align="center"><b>Client multi-protocollo:</b><br> Native support for RESP, REST, gRPC with SDKs for Python, Node, Go, C#, Java</td>
    </tr>
      <tr>
        <td align="center"><b>Scalability:</b><br>  Scalability up to the number of cores.
                                              <br>  Startup footprint < 10MB (embedded engine).
                                              <br>  WAL persistence + hybrid snapshot (similar to LMDB).</td>
        <td align="center"><b>Performance:</b><br>  Integrated monitoring with PerfCounters and real-time metrics.
                                              <br>  Auto-Tuning module for automatic optimisation.
                                              <br>  Competitive performance: GET p50 < 200 ns/op and Range 10K ~600–900 ns.</td>
    </tr>
</table>

# Why NEUROINDEX?

* **Extreme Performance**: Sub-microsecond (200ns typical) GET latency, throughput scaling linearly with CPU cores.

* **Modern Architecture**: Lock-free sharding, native multi-threading, zero-copy reads, advanced index algorithms (ART, Cuckoo Hash, Hopscotch).

* **Resilience & Persistence**: Non-blocking background snapshots, sharded WAL (Write-Ahead Log), robust recovery, basic clustering.

* **Multi-protocol Interfaces**: RESP (Redis protocol) compatibility, HTTP REST API, SDKs for Python, Go, NodeJS, .NET, and more.

* **Production Readiness**: Real resource limit enforcement, O(1) metrics, rate limiting, and baseline security (TLS, Auth Tokens).

# Operational Demo Overview
You’ll start the RESP server (port 6381), import 1M records via the Python client, create a snapshot from the CLI, then bring up the HTTP server (port 8080) pointing to the same persistence directory. Here’s the step-by-step.

##  Falsh Installation 
```bash
execute demo.sh
```

##  APT Installation 
```bash
echo "deb [trusted=yes] https://brainbaitdev.github.io/NeuroIndex/debian stable main" | sudo tee /etc/apt/sources.list.d/neuroindex.list
sudo apt update
sudo apt install neuroindex
 ```

##  Manual Installation

### 1. Compile your build
```bash
cargo build --release
```

### 2. Start NeuroIndex RESP Server
In a first terminal start the server:
```bash
./target/release/neuroindex-resp-server --port 6381 --shards 16 --capacity 65536 --log-level info --persistence-dir ./data
```

Verify the port is listening (optional):
```bash
lsof -i :6381 or ss -lntp | grep 6381
```

### 3. Load 1M records via Python client
In a second terminal run the batch import (5k per batch):
```bash
python3 clients/python/insert_1m_resp.py --host 0.0.0.0 --port 6381 --total 1000000 --batch-size 5000 --prefix account
```
**Tips:**
If you hit timeouts, lower --batch-size (e.g., 2000).
Watch the server terminal for backpressure or memory warnings.

### 4. Take a snapshot with neuroindex-cli
Start the CLI:
```bash
./target/release/neuroindex-cli --host 0.0.0.0 --port 6381
```
From the CLI prompt, run:
```bash
snapshot
```

<u>Notes:</u>
Wait for the completion message before proceeding.
The snapshot is written under ./data (as configured by the RESP server).
If writes are ongoing, the snapshot reflects the state at the command time

### 5. Start NeuroIndex HTTP Server
In a third terminal launch the process:
```bash
./target/release/neuroindex-http --port 8080 --shards 16 --capacity 65536 --log-level info --persistence-dir ./data
```
<br>

**Tips:**
if you to add a Bearer Token add the paramenter
```bash
--auth-token "my-secret-token"
```

Verify the port is up:
```bash
lsof -i :8080 or curl http://127.0.0.1:8080/api/v1/health (if a health endpoint is available)
```

## CHECK FOR MEMORY USAGE
```bash
ps -C neuroindex-http -o rss= | awk '{sum+=$1} END {printf "RAM: %.2f MB\n", sum/1024}'
```

## QUERY REST API FOR TEST

#### Health check
```bash
curl -s http://127.0.0.1:8080/api/v1/health | jq
```
#### Stats
```bash
curl -s http://127.0.0.1:8080/api/v1/stats | jq
```

#### GET single key
```bash
curl -s http://127.0.0.1:8080/api/v1/records/account:0050000 | jq
```

### Range query
```bash
curl -s "http://127.0.0.1:8080/api/v1/records/range?start=account:0000000&end=account:0100000&limit=50" | jq
```

### Range query where ts key contain 701

```bash
curl -s "http://127.0.0.1:8080/api/v1/records/range?start=account:0900000&end=account:0910000&ts=_701" | jq range query where ts key contain 701
```

#### Count aggregation
```bash
curl -s "http://127.0.0.1:8080/api/v1/aggregations/count?start=account:0000000&end=account:0100000" | jq
```

#### Bulk insert
```bash
curl -X POST http://127.0.0.1:8080/api/v1/records/bulk 
-H "Content-Type: application/json" 
-d '{"records":[{"key":"demo:001","value":{"name":"Test User 1","active":true}},{"key":"demo:002","value":{"name":"Test User 2","active":false}}]}' | jq
```

#### Update a value in case of Bearer Token
```bash
curl -X POST http://localhost:8080/api/v1/records/user:123 \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Mario"}}'
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


## CONTRIBUTING & FEEDBACK

We are actively seeking feedback from the community.

### What We Are Looking For:

1. **Architecture critique**: Should I have used X instead of Y?
2. **Rust vs C trade-off**: Did the complexity pay off for this use case?
3. **Performance methodology**: Are my benchmarks realistic or misleading?
4. **Missing critical patterns**: What design patterns am I overlooking?
5. **Code quality**: Where can I improve Rust idioms?

### How to Provide Feedback

#### For Technical Discussions
- 💬 **GitHub Discussions**: For architectural questions, design decisions
- 🐛 **Issues**: For bugs, performance problems, feature suggestions

#### For Code Contributions
- 🔧 **Pull Requests**: Welcome! Please open an issue first to discuss
- 📝 **Documentation PRs**: Always appreciated
- 🧪 **Benchmark improvements**: Especially interested in realistic workloads