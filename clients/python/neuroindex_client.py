#!/usr/bin/env python3
"""
NeuroIndex Python Client
Connects to NeuroIndex RESP protocol server
"""

import socket
import sys


class NeuroIndexClient:
    """Python client for NeuroIndex database using RESP protocol"""

    def __init__(self, host='localhost', port=6381):
        self.host = host
        self.port = port
        self.socket = None
        self.connect()

    def connect(self):
        """Connect to the NeuroIndex server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            print(f"✓ Connected to NeuroIndex at {self.host}:{self.port}")
        except Exception as e:
            print(f"✗ Failed to connect: {e}")
            sys.exit(1)

    def close(self):
        """Close the connection"""
        if self.socket:
            self.socket.close()
            print("✓ Connection closed")

    def _send_command(self, *args):
        """Send a RESP command to the server"""
        # Build RESP array
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"

        self.socket.sendall(cmd.encode())

    def _read_response(self):
        """Read and parse RESP response from server"""
        line = self._read_line()

        if not line:
            return None

        resp_type = line[0]
        data = line[1:]

        if resp_type == '+':  # Simple string
            return data
        elif resp_type == '-':  # Error
            raise Exception(f"Server error: {data}")
        elif resp_type == ':':  # Integer
            return int(data)
        elif resp_type == '$':  # Bulk string
            length = int(data)
            if length == -1:
                return None
            value = self.socket.recv(length).decode()
            self._read_line()  # Read trailing \r\n
            return value
        elif resp_type == '*':  # Array
            count = int(data)
            if count == -1:
                return None
            return [self._read_response() for _ in range(count)]
        else:
            raise Exception(f"Unknown RESP type: {resp_type}")

    def _read_line(self):
        """Read a line from the socket (until \\r\\n)"""
        line = b''
        while True:
            char = self.socket.recv(1)
            if not char:
                return None
            if char == b'\r':
                self.socket.recv(1)  # Read \n
                break
            line += char
        return line.decode()

    # Command methods

    def ping(self, message=None):
        """Test connection"""
        if message:
            self._send_command("PING", message)
        else:
            self._send_command("PING")
        return self._read_response()

    def echo(self, message):
        """Echo a message"""
        self._send_command("ECHO", message)
        return self._read_response()

    def set(self, key, value):
        """Set a key-value pair"""
        self._send_command("SET", key, value)
        return self._read_response()

    def get(self, key):
        """Get a value by key"""
        self._send_command("GET", key)
        return self._read_response()

    def delete(self, *keys):
        """Delete one or more keys"""
        self._send_command("DEL", *keys)
        return self._read_response()

    def exists(self, *keys):
        """Check if keys exist"""
        self._send_command("EXISTS", *keys)
        return self._read_response()

    def mget(self, *keys):
        """Get multiple values"""
        self._send_command("MGET", *keys)
        return self._read_response()

    def mset(self, *pairs):
        """Set multiple key-value pairs. Usage: mset('key1', 'val1', 'key2', 'val2')"""
        if len(pairs) % 2 != 0:
            raise ValueError("MSET requires an even number of arguments")
        self._send_command("MSET", *pairs)
        return self._read_response()

    def keys(self, pattern="*"):
        """Get all keys (pattern matching not implemented)"""
        self._send_command("KEYS", pattern)
        return self._read_response()

    def dbsize(self):
        """Get database size"""
        self._send_command("DBSIZE")
        return self._read_response()

    def info(self):
        """Get server info"""
        self._send_command("INFO")
        return self._read_response()


def main():
    """Example usage"""
    print("NeuroIndex Python Client Demo")
    print("=" * 50)

    # Connect
    client = NeuroIndexClient(host='localhost', port=6381)

    try:
        # Test ping
        print(f"\nPING: {client.ping()}")
        print(f"PING with message: {client.ping('Hello NeuroIndex!')}")

        # Set and get
        print(f"\nSET user:1 'Alice': {client.set('user:1', 'Alice')}")
        print(f"GET user:1: {client.get('user:1')}")

        # Multiple operations
        print(f"\nMSET: {client.mset('user:2', 'Bob', 'user:3', 'Charlie', 'user:4', 'David')}")
        print(f"MGET: {client.mget('user:1', 'user:2', 'user:3', 'user:4')}")

        # Exists and delete
        print(f"\nEXISTS user:1 user:5: {client.exists('user:1', 'user:5')}")
        print(f"DEL user:1: {client.delete('user:1')}")
        print(f"EXISTS user:1: {client.exists('user:1')}")

        # Database info
        print(f"\nDBSIZE: {client.dbsize()}")
        print(f"KEYS: {client.keys()}")

        # Server info
        print(f"\nINFO:\n{client.info()}")

        print("\n" + "=" * 50)
        print("✓ All operations completed successfully!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()