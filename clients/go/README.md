# NeuroIndex Go Client

Official Go client for NeuroIndex database.

## Installation

```bash
# Clone or copy the client file
cd clients/go

# Run the demo
go run neuroindex_client.go
```

## Basic Usage

```go
package main

import (
    "fmt"
    "log"
)

func main() {
    // Connect to NeuroIndex
    client, err := NewClient("localhost", 6381)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Set a key
    ok, err := client.Set("mykey", "myvalue")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("SET:", ok) // Output: OK

    // Get a key
    value, err := client.Get("mykey")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("GET:", value) // Output: myvalue
}
```

## Batch Operations

```go
// Insert 10,000 records efficiently
func insertRecords(client *NeuroIndexClient) error {
    batchSize := 100

    for i := 0; i < 10000; i += batchSize {
        pairs := make([]string, 0, batchSize*2)

        for j := i; j < i+batchSize && j < 10000; j++ {
            pairs = append(pairs,
                fmt.Sprintf("user:%d", j),
                fmt.Sprintf("User %d", j),
            )
        }

        _, err := client.MSet(pairs...)
        if err != nil {
            return err
        }
    }

    size, _ := client.DBSize()
    fmt.Printf("Inserted %d records\n", size)
    return nil
}
```

## Available Methods

| Method | Description | Return Type |
|--------|-------------|-------------|
| `Ping(message...)` | Test connection | `string, error` |
| `Echo(message)` | Echo message | `string, error` |
| `Set(key, value)` | Set key-value | `string, error` |
| `Get(key)` | Get value | `string, error` |
| `MSet(pairs...)` | Set multiple | `string, error` |
| `MGet(keys...)` | Get multiple | `[]interface{}, error` |
| `Del(keys...)` | Delete keys | `int64, error` |
| `Exists(keys...)` | Check existence | `int64, error` |
| `Keys(pattern...)` | List keys | `[]interface{}, error` |
| `DBSize()` | Database size | `int64, error` |
| `Info()` | Server info | `string, error` |

## Error Handling

All methods return errors that should be checked:

```go
value, err := client.Get("mykey")
if err != nil {
    log.Printf("Error getting key: %v", err)
    return
}

if value == "" {
    log.Println("Key not found")
}
```

## Connection Management

```go
// Create client
client, err := NewClient("localhost", 6381)
if err != nil {
    log.Fatal(err)
}

// Always close when done
defer client.Close()

// Or manual close
err = client.Close()
if err != nil {
    log.Printf("Error closing connection: %v", err)
}
```

## Dependencies

**None!** Uses Go standard library only:
- `net` - TCP connections
- `bufio` - Buffered I/O
- `fmt`, `strconv`, `strings` - String manipulation
- `errors` - Error handling

## Performance Tips

1. **Use batch operations** - `MSet`/`MGet` are much faster than individual operations
2. **Connection pooling** - Create a pool of clients for concurrent operations
3. **Error handling** - Always check errors to avoid silent failures

## Example: Connection Pool

```go
type ClientPool struct {
    clients chan *NeuroIndexClient
}

func NewClientPool(size int, host string, port int) (*ClientPool, error) {
    pool := &ClientPool{
        clients: make(chan *NeuroIndexClient, size),
    }

    for i := 0; i < size; i++ {
        client, err := NewClient(host, port)
        if err != nil {
            return nil, err
        }
        pool.clients <- client
    }

    return pool, nil
}

func (p *ClientPool) Get() *NeuroIndexClient {
    return <-p.clients
}

func (p *ClientPool) Put(client *NeuroIndexClient) {
    p.clients <- client
}
```
