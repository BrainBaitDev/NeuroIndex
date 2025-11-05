#!/usr/bin/env node
/**
 * NeuroIndex Node.js Client
 * Connects to NeuroIndex RESP protocol server
 */

const net = require('net');

class NeuroIndexClient {
    constructor(host = 'localhost', port = 6381) {
        this.host = host;
        this.port = port;
        this.socket = null;
        this.buffer = Buffer.alloc(0);
        this.callbacks = [];
    }

    connect() {
        return new Promise((resolve, reject) => {
            this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
                console.log(`✓ Connected to NeuroIndex at ${this.host}:${this.port}`);
                resolve();
            });

            this.socket.on('error', (err) => {
                console.error(`✗ Connection error: ${err.message}`);
                reject(err);
            });

            this.socket.on('data', (data) => {
                this.buffer = Buffer.concat([this.buffer, data]);
                this._processBuffer();
            });

            this.socket.on('close', () => {
                console.log('✓ Connection closed');
            });
        });
    }

    close() {
        if (this.socket) {
            this.socket.end();
        }
    }

    _sendCommand(...args) {
        return new Promise((resolve, reject) => {
            // Build RESP array
            let cmd = `*${args.length}\r\n`;
            for (const arg of args) {
                const argStr = String(arg);
                cmd += `$${Buffer.byteLength(argStr)}\r\n${argStr}\r\n`;
            }

            this.callbacks.push({ resolve, reject });
            this.socket.write(cmd);
        });
    }

    _processBuffer() {
        while (this.buffer.length > 0) {
            const result = this._parseResponse();
            if (result === null) {
                break; // Need more data
            }

            const callback = this.callbacks.shift();
            if (callback) {
                if (result instanceof Error) {
                    callback.reject(result);
                } else {
                    callback.resolve(result);
                }
            }
        }
    }

    _parseResponse() {
        if (this.buffer.length === 0) {
            return null;
        }

        const respType = String.fromCharCode(this.buffer[0]);
        const lineEnd = this.buffer.indexOf('\r\n');

        if (lineEnd === -1) {
            return null; // Need more data
        }

        const line = this.buffer.slice(1, lineEnd).toString();
        this.buffer = this.buffer.slice(lineEnd + 2);

        switch (respType) {
            case '+': // Simple string
                return line;

            case '-': // Error
                return new Error(`Server error: ${line}`);

            case ':': // Integer
                return parseInt(line, 10);

            case '$': { // Bulk string
                const length = parseInt(line, 10);
                if (length === -1) {
                    return null;
                }
                if (this.buffer.length < length + 2) {
                    // Put back the header, need more data
                    this.buffer = Buffer.concat([
                        Buffer.from(`$${line}\r\n`),
                        this.buffer
                    ]);
                    return null;
                }
                const value = this.buffer.slice(0, length).toString();
                this.buffer = this.buffer.slice(length + 2);
                return value;
            }

            case '*': { // Array
                const count = parseInt(line, 10);
                if (count === -1) {
                    return null;
                }
                const array = [];
                for (let i = 0; i < count; i++) {
                    const element = this._parseResponse();
                    if (element === null && this.buffer.length === 0) {
                        // Need more data, restore buffer
                        this.buffer = Buffer.concat([
                            Buffer.from(`*${line}\r\n`),
                            this.buffer
                        ]);
                        return null;
                    }
                    array.push(element);
                }
                return array;
            }

            default:
                return new Error(`Unknown RESP type: ${respType}`);
        }
    }

    // Command methods

    async ping(message) {
        if (message) {
            return this._sendCommand('PING', message);
        }
        return this._sendCommand('PING');
    }

    async echo(message) {
        return this._sendCommand('ECHO', message);
    }

    async set(key, value) {
        return this._sendCommand('SET', key, value);
    }

    async get(key) {
        return this._sendCommand('GET', key);
    }

    async del(...keys) {
        return this._sendCommand('DEL', ...keys);
    }

    async exists(...keys) {
        return this._sendCommand('EXISTS', ...keys);
    }

    async mget(...keys) {
        return this._sendCommand('MGET', ...keys);
    }

    async mset(...pairs) {
        if (pairs.length % 2 !== 0) {
            throw new Error('MSET requires an even number of arguments');
        }
        return this._sendCommand('MSET', ...pairs);
    }

    async keys(pattern = '*') {
        return this._sendCommand('KEYS', pattern);
    }

    async dbsize() {
        return this._sendCommand('DBSIZE');
    }

    async info() {
        return this._sendCommand('INFO');
    }
}

// Example usage
async function main() {
    console.log('NeuroIndex Node.js Client Demo');
    console.log('='.repeat(50));

    const client = new NeuroIndexClient('localhost', 6381);

    try {
        await client.connect();

        // Test ping
        console.log(`\nPING: ${await client.ping()}`);
        console.log(`PING with message: ${await client.ping('Hello NeuroIndex!')}`);

        // Set and get
        console.log(`\nSET user:1 'Alice': ${await client.set('user:1', 'Alice')}`);
        console.log(`GET user:1: ${await client.get('user:1')}`);

        // Multiple operations
        console.log(`\nMSET: ${await client.mset('user:2', 'Bob', 'user:3', 'Charlie', 'user:4', 'David')}`);
        console.log(`MGET: ${JSON.stringify(await client.mget('user:1', 'user:2', 'user:3', 'user:4'))}`);

        // Exists and delete
        console.log(`\nEXISTS user:1 user:5: ${await client.exists('user:1', 'user:5')}`);
        console.log(`DEL user:1: ${await client.del('user:1')}`);
        console.log(`EXISTS user:1: ${await client.exists('user:1')}`);

        // Database info
        console.log(`\nDBSIZE: ${await client.dbsize()}`);
        console.log(`KEYS: ${JSON.stringify(await client.keys())}`);

        // Server info
        console.log(`\nINFO:\n${await client.info()}`);

        console.log('\n' + '='.repeat(50));
        console.log('✓ All operations completed successfully!');

    } catch (error) {
        console.error(`\n✗ Error: ${error.message}`);
    } finally {
        client.close();
    }
}

// Run demo if executed directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = NeuroIndexClient;
