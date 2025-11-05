# NeuroIndex Client Connectors

Official client libraries for connecting to NeuroIndex from multiple programming languages.

Default ports
- RESP protocol: 6381 (updated from 6379)
- HTTP REST API: 8080

## Available Clients

| Language | File | Dependencies | Status |
|----------|------|--------------|--------|
| **Python** | `python/neuroindex_client.py` | None (stdlib only) | ✅ Ready |
| **Node.js** | `nodejs/neuroindex-client.js` | None (net module) | ✅ Ready |
| **.NET/C#** | `dotnet/NeuroIndexClient.cs` | .NET 5.0+ | ✅ Ready |
| **Go** | `go/neuroindex_client.go` | Go 1.21+ | ✅ Ready |
| **Shell** | `curl/examples.sh` | netcat/nc | ✅ Ready |
| **Rust CLI** | `../crates/resp-cli` | Rust toolchain | ✅ Ready |

## Quick Start

### Python

```bash
cd python
python3 neuroindex_client.py
```

### Node.js

```bash
cd nodejs
node neuroindex-client.js
```

### .NET

```bash
cd dotnet
dotnet run
```

### Go

```bash
cd go
go run neuroindex_client.go
```

### Shell/netcat

```bash
cd curl
chmod +x examples.sh
./examples.sh
```

### Rust CLI

```bash
cargo run --release -p neuroindex-cli -- --host 127.0.0.1 --port 6381 PING
```

Oppure costruisci il binario e lancialo direttamente:

```bash
cargo build --release -p neuroindex-cli
./target/release/neuroindex-cli --host 127.0.0.1 --port 6381
```

## Features

All clients support:
- ✅ **PING** - Connection testing
- ✅ **SET/GET** - Basic key-value operations
- ✅ **MSET/MGET** - Batch operations
- ✅ **DEL** - Key deletion
- ✅ **EXISTS** - Check key existence
- ✅ **KEYS** - List all keys
- ✅ **DBSIZE** - Database size
- ✅ **INFO** - Server statistics
- ✅ **FLUSHDB** - Delete all keys
- ✅ **ECHO** - Echo messages
- ✅ **FLUSHWAL / SNAPSHOT** - Persist data to disk when persistence is enabled

## Protocol

All clients implement the RESP (REdis Serialization Protocol) for compatibility with:
- Standard Redis client libraries
- Wire protocol tools (netcat, telnet)
- Custom implementations

## Documentation

See [CLIENT_USAGE.md](../CLIENT_USAGE.md) for complete usage guide with examples.

## License

Apache 2.0 - Same as NeuroIndex core
