#!/usr/bin/env python3
"""
Insert 1 million records into NeuroIndex via RESP protocol.

Records are inserted in configurable batches with optional pause between
each batch to simulate staggered ingestion.
"""

import argparse
import json
import sys
import time

from neuroindex_client import NeuroIndexClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Insert staggered batches of records into NeuroIndex (RESP)."
    )
    parser.add_argument("--host", default="localhost", help="NeuroIndex host (default: localhost)")
    parser.add_argument("--port", type=int, default=6381, help="NeuroIndex port (default: 6381)")
    parser.add_argument("--total", type=int, default=1_000_000, help="Total records to insert")
    parser.add_argument("--batch-size", type=int, default=5_000, help="Records per batch (default: 5000)")
    parser.add_argument(
        "--batch-sleep",
        type=float,
        default=0.05,
        help="Seconds to sleep between batches (default: 0.05)",
    )
    parser.add_argument(
        "--prefix",
        default="user",
        help="Key prefix to use for inserted records (default: user)",
    )
    return parser.parse_args()


def build_pairs(start: int, end: int, prefix: str) -> list[str]:
    pairs: list[str] = []
    for i in range(start, end):
        key = f"{prefix}:{i:07d}"
        value = json.dumps(
            {
                "id": i,
                "username": f"{prefix}_{i}",
                "score": (i * 37) % 10_000,
                "tier": "gold" if i % 10 == 0 else "standard",
                "ts": time.time(),
            }
        )
        pairs.extend([key, value])
    return pairs


def insert_records(args: argparse.Namespace) -> None:
    client = NeuroIndexClient(host=args.host, port=args.port)
    total_inserted = 0
    start_time = time.time()

    try:
        for batch_start in range(0, args.total, args.batch_size):
            batch_end = min(batch_start + args.batch_size, args.total)
            pairs = build_pairs(batch_start, batch_end, args.prefix)

            try:
                client.mset(*pairs)
            except Exception as err:
                print(f"✗ Error inserting batch {batch_start}-{batch_end}: {err}")
                sys.exit(1)

            total_inserted += batch_end - batch_start

            if total_inserted % (args.batch_size * 10) == 0 or total_inserted == args.total:
                elapsed = time.time() - start_time
                rate = total_inserted / elapsed if elapsed > 0 else 0.0
                print(f"✓ Inserted {total_inserted:,} records (avg {rate:,.0f} rec/s)")

            if args.batch_sleep > 0 and total_inserted < args.total:
                time.sleep(args.batch_sleep)

        elapsed = time.time() - start_time
        print("\nInsertion complete")
        print(f"  Total records: {total_inserted:,}")
        print(f"  Total time: {elapsed:.2f}s")
        print(f"  Avg throughput: {total_inserted / elapsed:,.0f} records/s")
    finally:
        client.close()


def main() -> None:
    args = parse_args()
    if args.batch_size <= 0:
        print("Batch size must be positive")
        sys.exit(2)
    if args.total <= 0:
        print("Total records must be positive")
        sys.exit(2)
    insert_records(args)


if __name__ == "__main__":
    main()
