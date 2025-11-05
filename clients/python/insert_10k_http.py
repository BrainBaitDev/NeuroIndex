#!/usr/bin/env python3
"""
Example: Insert 10,000 records via HTTP REST API
Demonstrates batch insertion for optimal performance
"""

from neuroindex_http_client import NeuroIndexHTTPClient
import time


def insert_10k_records():
    """Insert 10,000 records using bulk insert"""
    print("Inserting 10,000 records via HTTP REST API")
    print("=" * 50)

    client = NeuroIndexHTTPClient()

    try:
        # Method 1: Single bulk insert (FASTEST)
        print("\nMethod 1: Single bulk insert...")
        start = time.time()

        records = [
            {
                "key": f"user:{i:05d}",
                "value": {
                    "name": f"User {i}",
                    "email": f"user{i}@example.com",
                    "score": i * 10,
                    "active": i % 2 == 0
                }
            }
            for i in range(10000)
        ]

        result = client.bulk_insert(records)
        elapsed = time.time() - start

        print(f"✓ Inserted: {result['inserted']} records")
        print(f"✗ Failed: {len(result['failed'])} records")
        print(f"⏱️  Time: {elapsed:.2f} seconds")
        print(f"📊 Throughput: {result['inserted'] / elapsed:.0f} records/sec")

        # Verify
        stats = client.stats()
        print(f"\n✓ Total records in database: {stats['total_keys']}")

        # Method 2: Batched inserts (alternative for larger datasets)
        print("\n" + "=" * 50)
        print("Method 2: Batched inserts (100 records/batch)...")

        # Clear by inserting new dataset
        start = time.time()
        batch_size = 100
        total_inserted = 0

        for i in range(0, 10000, batch_size):
            batch = [
                {
                    "key": f"batch_user:{j:05d}",
                    "value": {
                        "name": f"Batch User {j}",
                        "batch": i // batch_size
                    }
                }
                for j in range(i, min(i + batch_size, 10000))
            ]

            result = client.bulk_insert(batch)
            total_inserted += result['inserted']

            if (i // batch_size) % 10 == 0:
                print(f"  Progress: {i + batch_size}/10000 records...")

        elapsed = time.time() - start
        print(f"\n✓ Inserted: {total_inserted} records")
        print(f"⏱️  Time: {elapsed:.2f} seconds")
        print(f"📊 Throughput: {total_inserted / elapsed:.0f} records/sec")

        # Final stats
        stats = client.stats()
        print(f"\n✓ Final total records: {stats['total_keys']}")

        # Query examples
        print("\n" + "=" * 50)
        print("Query Examples:")

        # Range query
        results = client.range_query(start="user:00100", end="user:00110", limit=5)
        print(f"\nRange query (user:00100 to user:00110, limit 5):")
        for record in results:
            print(f"  - {record['key']}: {record['value']['name']}")

        # Count aggregation
        count = client.count(start="user:00000", end="user:09999")
        print(f"\nCount (user:00000 to user:09999): {count}")

        print("\n" + "=" * 50)
        print("✓ Demo completed successfully!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    insert_10k_records()
