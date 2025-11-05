// NeuroIndex Go Client
// Connects to NeuroIndex RESP protocol server

package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// NeuroIndexClient represents a connection to NeuroIndex server
type NeuroIndexClient struct {
	host   string
	port   int
	conn   net.Conn
	reader *bufio.Reader
}

// NewClient creates a new NeuroIndex client
func NewClient(host string, port int) (*NeuroIndexClient, error) {
	client := &NeuroIndexClient{
		host: host,
		port: port,
	}
	err := client.Connect()
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Connect establishes connection to the server
func (c *NeuroIndexClient) Connect() error {
	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	fmt.Printf("✓ Connected to NeuroIndex at %s\n", addr)
	return nil
}

// Close closes the connection
func (c *NeuroIndexClient) Close() error {
	if c.conn != nil {
		fmt.Println("✓ Connection closed")
		return c.conn.Close()
	}
	return nil
}

// sendCommand sends a RESP command to the server
func (c *NeuroIndexClient) sendCommand(args ...string) error {
	// Build RESP array
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	_, err := c.conn.Write([]byte(cmd))
	return err
}

// readResponse reads and parses RESP response from server
func (c *NeuroIndexClient) readResponse() (interface{}, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, errors.New("empty response")
	}

	respType := line[0]
	data := line[1:]

	switch respType {
	case '+': // Simple string
		return data, nil

	case '-': // Error
		return nil, fmt.Errorf("server error: %s", data)

	case ':': // Integer
		return strconv.ParseInt(data, 10, 64)

	case '$': // Bulk string
		length, err := strconv.Atoi(data)
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil
		}

		buf := make([]byte, length+2) // +2 for \r\n
		_, err = c.reader.Read(buf)
		if err != nil {
			return nil, err
		}
		return string(buf[:length]), nil

	case '*': // Array
		count, err := strconv.Atoi(data)
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}

		array := make([]interface{}, count)
		for i := 0; i < count; i++ {
			element, err := c.readResponse()
			if err != nil {
				return nil, err
			}
			array[i] = element
		}
		return array, nil

	default:
		return nil, fmt.Errorf("unknown RESP type: %c", respType)
	}
}

// Command methods

// Ping tests the connection
func (c *NeuroIndexClient) Ping(message ...string) (string, error) {
	if len(message) > 0 {
		err := c.sendCommand("PING", message[0])
		if err != nil {
			return "", err
		}
	} else {
		err := c.sendCommand("PING")
		if err != nil {
			return "", err
		}
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// Echo echoes a message
func (c *NeuroIndexClient) Echo(message string) (string, error) {
	err := c.sendCommand("ECHO", message)
	if err != nil {
		return "", err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// Set sets a key-value pair
func (c *NeuroIndexClient) Set(key, value string) (string, error) {
	err := c.sendCommand("SET", key, value)
	if err != nil {
		return "", err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// Get gets a value by key
func (c *NeuroIndexClient) Get(key string) (string, error) {
	err := c.sendCommand("GET", key)
	if err != nil {
		return "", err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", nil
	}
	return resp.(string), nil
}

// Del deletes one or more keys
func (c *NeuroIndexClient) Del(keys ...string) (int64, error) {
	args := append([]string{"DEL"}, keys...)
	err := c.sendCommand(args...)
	if err != nil {
		return 0, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// Exists checks if keys exist
func (c *NeuroIndexClient) Exists(keys ...string) (int64, error) {
	args := append([]string{"EXISTS"}, keys...)
	err := c.sendCommand(args...)
	if err != nil {
		return 0, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// MGet gets multiple values
func (c *NeuroIndexClient) MGet(keys ...string) ([]interface{}, error) {
	args := append([]string{"MGET"}, keys...)
	err := c.sendCommand(args...)
	if err != nil {
		return nil, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}
	return resp.([]interface{}), nil
}

// MSet sets multiple key-value pairs
func (c *NeuroIndexClient) MSet(pairs ...string) (string, error) {
	if len(pairs)%2 != 0 {
		return "", errors.New("MSET requires an even number of arguments")
	}

	args := append([]string{"MSET"}, pairs...)
	err := c.sendCommand(args...)
	if err != nil {
		return "", err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// Keys gets all keys
func (c *NeuroIndexClient) Keys(pattern ...string) ([]interface{}, error) {
	p := "*"
	if len(pattern) > 0 {
		p = pattern[0]
	}

	err := c.sendCommand("KEYS", p)
	if err != nil {
		return nil, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}
	return resp.([]interface{}), nil
}

// DBSize gets database size
func (c *NeuroIndexClient) DBSize() (int64, error) {
	err := c.sendCommand("DBSIZE")
	if err != nil {
		return 0, err
	}

	resp, err := c.readResponse()
	if err != nil {
		return 0, err
	}
	return resp.(int64), nil
}

// Info gets server info
func (c *NeuroIndexClient) Info() (string, error) {
	err := c.sendCommand("INFO")
	if err != nil {
		return "", err
	}

	resp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

// Example usage
func main() {
	fmt.Println("NeuroIndex Go Client Demo")
	fmt.Println(strings.Repeat("=", 50))

	// Connect
	client, err := NewClient("localhost", 6381)
	if err != nil {
		fmt.Printf("✗ Failed to connect: %v\n", err)
		return
	}
	defer client.Close()

	// Test ping
	pong, _ := client.Ping()
	fmt.Printf("\nPING: %s\n", pong)

	pongMsg, _ := client.Ping("Hello NeuroIndex!")
	fmt.Printf("PING with message: %s\n", pongMsg)

	// Set and get
	ok, _ := client.Set("user:1", "Alice")
	fmt.Printf("\nSET user:1 'Alice': %s\n", ok)

	value, _ := client.Get("user:1")
	fmt.Printf("GET user:1: %s\n", value)

	// Multiple operations
	ok, _ = client.MSet("user:2", "Bob", "user:3", "Charlie", "user:4", "David")
	fmt.Printf("\nMSET: %s\n", ok)

	values, _ := client.MGet("user:1", "user:2", "user:3", "user:4")
	fmt.Printf("MGET: %v\n", values)

	// Exists and delete
	count, _ := client.Exists("user:1", "user:5")
	fmt.Printf("\nEXISTS user:1 user:5: %d\n", count)

	deleted, _ := client.Del("user:1")
	fmt.Printf("DEL user:1: %d\n", deleted)

	count, _ = client.Exists("user:1")
	fmt.Printf("EXISTS user:1: %d\n", count)

	// Database info
	size, _ := client.DBSize()
	fmt.Printf("\nDBSIZE: %d\n", size)

	keys, _ := client.Keys()
	fmt.Printf("KEYS: %v\n", keys)

	// Server info
	info, _ := client.Info()
	fmt.Printf("\nINFO:\n%s\n", info)

	fmt.Println()
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("✓ All operations completed successfully!")
}
