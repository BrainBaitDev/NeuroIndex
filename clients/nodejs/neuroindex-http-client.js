#!/usr/bin/env node
/**
 * NeuroIndex Node.js HTTP REST API Client
 * Connects to NeuroIndex HTTP server (port 8080)
 */

const http = require('http');
const https = require('https');
const { URL } = require('url');

class NeuroIndexHTTPClient {
    constructor(baseUrl = 'http://localhost:8080/api/v1') {
        this.baseUrl = baseUrl;
        console.log(`✓ Connected to NeuroIndex HTTP API at ${baseUrl}`);
    }

    async _request(method, path, body = null, params = null) {
        let url = `${this.baseUrl}${path}`;

        // Add query parameters
        if (params) {
            const query = new URLSearchParams(params).toString();
            url += `?${query}`;
        }

        const parsedUrl = new URL(url);
        const isHttps = parsedUrl.protocol === 'https:';
        const httpModule = isHttps ? https : http;

        return new Promise((resolve, reject) => {
            const options = {
                hostname: parsedUrl.hostname,
                port: parsedUrl.port || (isHttps ? 443 : 80),
                path: parsedUrl.pathname + parsedUrl.search,
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                }
            };

            const req = httpModule.request(options, (res) => {
                let data = '';

                res.on('data', (chunk) => {
                    data += chunk;
                });

                res.on('end', () => {
                    if (res.statusCode === 204) {
                        resolve({ statusCode: 204 });
                    } else if (res.statusCode >= 200 && res.statusCode < 300) {
                        try {
                            resolve(JSON.parse(data));
                        } catch (e) {
                            resolve(data);
                        }
                    } else if (res.statusCode === 404) {
                        resolve(null);
                    } else {
                        reject(new Error(`HTTP ${res.statusCode}: ${data}`));
                    }
                });
            });

            req.on('error', (err) => {
                reject(err);
            });

            if (body) {
                req.write(JSON.stringify(body));
            }

            req.end();
        });
    }

    async health() {
        return this._request('GET', '/api/v1/health');
    }

    async stats() {
        return this._request('GET', '/api/v1/stats');
    }

    async put(key, value) {
        return this._request('POST', `/api/v1/records/${key}`, { value });
    }

    async get(key) {
        const result = await this._request('GET', `/api/v1/records/${key}`);
        return result ? result.value : null;
    }

    async delete(key) {
        const result = await this._request('DELETE', `/api/v1/records/${key}`);
        return result.statusCode === 204;
    }

    async bulkInsert(records) {
        return this._request('POST', '/api/v1/records/bulk', { records });
    }

    async rangeQuery(options = {}) {
        const { start, end, limit } = options;
        const params = {};
        if (start) params.start = start;
        if (end) params.end = end;
        if (limit) params.limit = limit;

        const result = await this._request('GET', '/api/v1/records/range', null, params);
        return result.results;
    }

    async count(options = {}) {
        const { start, end } = options;
        const params = {};
        if (start) params.start = start;
        if (end) params.end = end;

        const result = await this._request('GET', '/api/v1/aggregations/count', null, params);
        return result.count;
    }

    async executeSql(query) {
        // POST /api/v1/sql with { query: "..." }
        return this._request('POST', '/api/v1/sql', { query });
    }

    close() {
        console.log('✓ Session closed');
    }
}

// Example usage
async function main() {
    console.log('NeuroIndex HTTP Client Demo');
    console.log('='.repeat(50));

    const client = new NeuroIndexHTTPClient('http://127.0.0.1:8080/api/v1');

    try {
        // Test health
        const health = await client.health();
        console.log(`\nHealth: ${JSON.stringify(health)}`);

        // Set and get
        const putResult = await client.put('user:1', { name: 'Alice', age: 30 });
        console.log(`\nPUT user:1 = Alice: ${JSON.stringify(putResult)}`);

        const getValue = await client.get('user:1');
        console.log(`GET user:1: ${JSON.stringify(getValue)}`);

        // Bulk insert
        const records = [];
        for (let i = 2; i <= 100; i++) {
            records.push({
                key: `user:${i}`,
                value: { name: `User ${i}`, score: i * 10 }
            });
        }

        const bulkResult = await client.bulkInsert(records);
        console.log(`\nBulk insert: inserted=${bulkResult.inserted}, failed=${bulkResult.failed.length}`);

        // Range query
        const rangeResults = await client.rangeQuery({
            start: 'user:10',
            end: 'user:20',
            limit: 5
        });
        console.log(`\nRange query (limit 5): ${rangeResults.length} results`);
        for (let i = 0; i < Math.min(3, rangeResults.length); i++) {
            console.log(`  - ${rangeResults[i].key}: ${JSON.stringify(rangeResults[i].value)}`);
        }

        // Count
        const count = await client.count({ start: 'user:1', end: 'user:99' });
        console.log(`\nCount (user:1 to user:99): ${count}`);

        // Stats
        const stats = await client.stats();
        console.log(`\nStats: ${JSON.stringify(stats)}`);

    // Example: SQL over HTTP
    const sqlResult = await client.executeSql("SELECT key, value FROM kv WHERE value LIKE 'User %' LIMIT 5");
    console.log('\nSQL result columns:', sqlResult.columns);
    (sqlResult.rows || []).slice(0,3).forEach(r => console.log('  -', JSON.stringify(r)));

        // Delete
        const deleted = await client.delete('user:1');
        console.log(`\nDelete user:1: ${deleted}`);

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

module.exports = NeuroIndexHTTPClient;
