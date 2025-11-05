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

    def __init__(self, base_url: str = "http://localhost:8080/api/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        print(f"{CHECK_MARK} Connected to NeuroIndex HTTP API at {base_url}")

    def close(self):
        """Close the session"""
        self.session.close()
        print(f"{CHECK_MARK} Session closed")

    def health(self) -> Dict[str, str]:
        """Check server health"""
        url = f"{self.base_url}/health"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        url = f"{self.base_url}/stats"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def put(self, key: str, value: Any) -> Dict[str, Any]:
        """Set a key-value pair"""
        url = f"{self.base_url}/records/{key}"
        response = self.session.post(url, json={"value": value})
        response.raise_for_status()
        return response.json()

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        url = f"{self.base_url}/records/{key}"
        response = self.session.get(url)

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()["value"]

    def delete(self, key: str) -> bool:
        """Delete a key"""
        url = f"{self.base_url}/records/{key}"
        response = self.session.delete(url)
        response.raise_for_status()
        return response.status_code == 204

    def bulk_insert(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk insert multiple records
        records: list of {"key": str, "value": any}
        """
        url = f"{self.base_url}/records/bulk"
        response = self.session.post(url, json={"records": records})
        response.raise_for_status()
        return response.json()

    def range_query(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
        username_contains: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query a range of keys with optional substring filter on username"""
        url = f"{self.base_url}/records/range"
        params = {}

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if limit:
            params["limit"] = limit
        if username_contains:
            params["username_contains"] = username_contains

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()["results"]

    def count(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        username_contains: Optional[str] = None,
    ) -> int:
        """Count records in a range with optional substring filter on username"""
        url = f"{self.base_url}/aggregations/count"
        params = {}

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if username_contains:
            params["username_contains"] = username_contains

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()["count"]


def main():
    """Example usage"""
    print("NeuroIndex HTTP Client Demo")
    print("=" * 50)

    # Connect
    client = NeuroIndexHTTPClient(base_url="http://localhost:8080/api/v1")

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

        filtered = client.range_query(
            start="user:10",
            end="user:200",
            limit=5,
            username_contains="_1",
        )
        print(f"\nFiltered range (username contains '_1'): {len(filtered)} results")

        # Count
        count = client.count(start="user:1", end="user:99")
        print(f"\nCount (user:1 to user:99): {count}")
        filtered_count = client.count(
            start="user:1",
            end="user:999",
            username_contains="_1",
        )
        print(f"Count with username contains '_1': {filtered_count}")

        # Stats
        stats = client.stats()
        print(f"\nStats: {stats}")

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
