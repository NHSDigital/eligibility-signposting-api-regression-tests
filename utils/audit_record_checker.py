from __future__ import annotations

import argparse
import gzip
import io
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterator

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_iso_datetime(value: str) -> datetime:
    """
    Parse an ISO-like datetime string into a timezone-aware UTC datetime.

    Accepted examples:
      2026-03-31T07:00:00Z
      2026-03-31T07:00:00+00:00
      2026-03-31 07:00:00+00:00
    """
    value = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        raise ValueError(
            f"Datetime '{value}' must include a timezone, e.g. +00:00 or Z"
        )

    return dt.astimezone(timezone.utc)


def time_ranges(
    start: datetime, end: datetime, interval_minutes: int
) -> Iterator[tuple[datetime, datetime]]:
    """
    Yield consecutive time intervals of size `interval_minutes` covering [start, end).
    """
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be greater than 0")

    current = start
    delta = timedelta(minutes=interval_minutes)

    while current < end:
        nxt = min(current + delta, end)
        yield current, nxt
        current = nxt


def iter_s3_objects_in_time_range(
    s3_client,
    bucket: str,
    start: datetime,
    end: datetime,
    prefix: str | None = None,
) -> Iterator[dict]:
    """
    Yield S3 objects whose LastModified is within [start, end).
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    paginate_kwargs = {"Bucket": bucket}
    if prefix:
        paginate_kwargs["Prefix"] = prefix

    for page in paginator.paginate(**paginate_kwargs):
        for obj in page.get("Contents", []):
            last_modified = obj["LastModified"].astimezone(timezone.utc)
            if start <= last_modified < end:
                yield obj


def count_ndjson_records_from_bytes(raw_bytes: bytes, key: str) -> int:
    """
    Count records in a file assumed to be newline-delimited JSON.

    Supports plain text and .gz content.
    Counts one JSON object per non-empty line.
    """
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz:
            text = gz.read().decode("utf-8")
    else:
        text = raw_bytes.decode("utf-8")

    count = 0
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue

        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON in object '{key}' at line {line_number}: {exc}"
            ) from exc

        if isinstance(parsed, dict):
            count += 1
        else:
            raise ValueError(
                f"Expected JSON object per line in '{key}', "
                f"got {type(parsed).__name__} at line {line_number}"
            )

    return count


def count_records_in_object(s3_client, bucket: str, key: str) -> int:
    """
    Download an S3 object and count audit records inside it.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()
    return count_ndjson_records_from_bytes(raw_bytes, key)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Count audit records in S3 objects over a time range in configurable intervals."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--start",
        required=True,
        help="Start datetime (inclusive), e.g. 2026-03-31T07:00:00Z",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End datetime (exclusive), e.g. 2026-03-31T10:00:00Z",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional S3 prefix to narrow the search",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Optional AWS region, e.g. eu-west-2",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=60,
        help="Interval size in minutes (default: 60)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-object counts as well as interval totals",
    )

    args = parser.parse_args()

    start = parse_iso_datetime(args.start)
    end = parse_iso_datetime(args.end)

    if end <= start:
        raise ValueError("--end must be later than --start")

    if args.interval_minutes <= 0:
        raise ValueError("--interval-minutes must be greater than 0")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3_client = session.client("s3")

    grand_total = 0
    interval_totals: dict[str, int] = defaultdict(int)
    interval_file_counts: dict[str, int] = defaultdict(int)

    for interval_start, interval_end in time_ranges(
        start, end, interval_minutes=args.interval_minutes
    ):
        interval_label = f"{interval_start.isoformat()} -> {interval_end.isoformat()}"
        interval_total = 0
        interval_object_count = 0

        print(f"\nProcessing interval: {interval_label}")

        for obj in iter_s3_objects_in_time_range(
            s3_client=s3_client,
            bucket=args.bucket,
            start=interval_start,
            end=interval_end,
            prefix=args.prefix,
        ):
            key = obj["Key"]
            last_modified = obj["LastModified"].astimezone(timezone.utc)

            try:
                record_count = count_records_in_object(s3_client, args.bucket, key)
            except (ClientError, BotoCoreError, ValueError, OSError) as exc:
                print(f"  ERROR reading {key}: {exc}")
                continue

            interval_total += record_count
            interval_object_count += 1

            if args.verbose:
                print(
                    f"  {key} | LastModified={last_modified.isoformat()} | records={record_count}"
                )

        interval_totals[interval_label] = interval_total
        interval_file_counts[interval_label] = interval_object_count
        grand_total += interval_total

        print(
            f"Interval total: {interval_total} audit records across {interval_object_count} object(s)"
        )

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for interval_label in interval_totals:
        objects = interval_file_counts[interval_label]
        records = interval_totals[interval_label]

        print(f"{interval_label} | " f"objects={objects} | " f"records={records}")

    print("-" * 80)
    print(f"GRAND TOTAL AUDIT RECORDS: {grand_total}")


if __name__ == "__main__":
    main()
