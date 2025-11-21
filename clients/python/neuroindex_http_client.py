#!/usr/bin/env python3
"""
NeuroIndex Python HTTP REST API Client
Connects to NeuroIndex HTTP server (port 8080)
"""

import requests
import json
import sys
from typing import Any, Dict, List, Optional

# Check if terminal supports UTF-8
try:
    '\u2713'.encode(sys.stdout.encoding or 'utf-8')
    CHECK_MARK = '\u2713'
    X_MARK = '\u2717'
except (UnicodeEncodeError, AttributeError, LookupError):
    CHECK_MARK = '[OK]'
    X_MARK = '[ERROR]'


class NeuroIndexHTTPClient:
    """Python client for NeuroIndex database using HTTP REST API"""

    def __init__(self, base_url: str = "http://localhost:8080"):
        # Ensure base_url doesn't have trailing slash
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        print(f"{CHECK_MARK} Connected to NeuroIndex HTTP API at {base_url}")

    def close(self):
        """Close the session"""
        self.session.close()
        print(f"{CHECK_MARK} Session closed")

    def health(self) -> Dict[str, str]:
        """Check server health"""
        url = f"{self.base_url}/api/v1/health"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        url = f"{self.base_url}/api/v1/stats"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def put(self, key: str, value: str) -> bool:
        """Insert or update a key-value pair."""
        url = f"{self.base_url}/api/v1/records/{key}"
        payload = {"value": value}
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return True

    def get(self, key: str) -> Optional[str]:
        """Retrieve a value by key."""
        url = f"{self.base_url}/api/v1/records/{key}"
        response = self.session.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json().get("value")

    def delete(self, key: str) -> bool:
        """Delete a key-value pair."""
        url = f"{self.base_url}/api/v1/records/{key}"
        response = self.session.delete(url)
        response.raise_for_status()
        return True

    def bulk_insert(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk insert multiple key-value pairs."""
        url = f"{self.base_url}/api/v1/records/bulk"
        payload = {"records": records}
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def range_query(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Query a range of keys."""
        url = f"{self.base_url}/api/v1/records/range"
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if limit:
            params["limit"] = limit
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])

    def count(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> int:
        """Count records in a range"""
        url = f"{self.base_url}/api/v1/aggregations/count"
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()["count"]

    def execute_sql(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query against the HTTP SQL endpoint.

        Returns the parsed JSON response which typically includes
        `columns`, `rows` and `rows_affected` depending on the query.
        """
        url = f"{self.base_url}/api/v1/sql"
        payload = {"query": query}
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()


def main():
    """Example usage"""
    print("NeuroIndex HTTP Client Demo")
    print("=" * 50)

    # Connect
    client = NeuroIndexHTTPClient(base_url="http://localhost:8080")

    try:
        # Test health
        health = client.health()
        print(f"\nHealth: {health}")

        # Set and get
        print(f"\nPUT user:1 = Alice: {client.put('user:1', {'name': 'Alice', 'age': 30})}")
        print(f"GET user:1: {client.get('user:1')}")

        # Bulk insert
        records = [
            {"key": f"user:{i}", "value": {"name": f"User {i}", "score": i * 10}}
            for i in range(2, 101)
        ]
        result = client.bulk_insert(records)
        print(f"\nBulk insert: inserted={result['inserted']}, failed={len(result['failed'])}")

        # Range query
        range_results = client.range_query(start="user:10", end="user:20", limit=5)
        print(f"\nRange query (limit 5): {len(range_results)} results")
        for record in range_results[:3]:
            print(f"  - {record['key']}: {record['value']}")

        # Count
        count = client.count(start="user:1", end="user:99")
        print(f"\nCount (user:1 to user:99): {count}")

        # Stats
        stats = client.stats()
        print(f"\nStats: {stats}")

        # Example: SQL query (HTTP)
        sql_result = client.execute_sql("SELECT key, value FROM kv WHERE value LIKE 'User %' LIMIT 5")
        print(f"\nSQL result columns: {sql_result.get('columns')}")
        for row in sql_result.get('rows', [])[:3]:
            print(f"  - {row}")

        # Delete
        deleted = client.delete("user:1")
        print(f"\nDelete user:1: {deleted}")

        print("\n" + "=" * 50)
        print(f"{CHECK_MARK} All operations completed successfully!")

    except Exception as e:
        print(f"\n{X_MARK} Error: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
