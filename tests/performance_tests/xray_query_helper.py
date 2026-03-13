import json
import logging
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APPLICATION_PREFIXES = (
    "local:CampaignRepo.",
    "local:ConsumerMappingRepo.",
    "local:PersonRepo.",
    "local:SecretRepo.",
    "local:AuditService.",
)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0

    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * (pct / 100.0)
    floor_index = math.floor(k)
    ceil_index = math.ceil(k)

    if floor_index == ceil_index:
        return float(sorted_values[int(k)])

    floor_value = sorted_values[floor_index]
    ceil_value = sorted_values[ceil_index]
    return float(floor_value + (ceil_value - floor_value) * (k - floor_index))


def chunked(items: list[str], size: int = 5) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def is_application_subsegment(name: str) -> bool:
    return name.startswith(APPLICATION_PREFIXES)


def normalise_subsegment_name(subsegment: dict[str, Any]) -> str:
    name = str(subsegment.get("name") or "unknown")
    namespace = subsegment.get("namespace")
    aws_block = subsegment.get("aws") or {}

    operation = aws_block.get("operation")
    if operation:
        return f"{name}:{operation}"

    if namespace:
        return f"{namespace}:{name}"

    return name


def subsegment_duration_ms(node: dict[str, Any]) -> float:
    start_time = node.get("start_time")
    end_time = node.get("end_time")

    if start_time is None or end_time is None:
        return 0.0

    return max(0.0, (float(end_time) - float(start_time)) * 1000.0)


def walk_subsegments(node: dict[str, Any]) -> list[dict[str, Any]]:
    subsegments = []

    for child in node.get("subsegments", []) or []:
        subsegments.append(child)
        subsegments.extend(walk_subsegments(child))

    return subsegments


def get_trace_summaries(
    xray_client: Any,
    start_time: datetime,
    end_time: datetime,
    filter_expression: str = "",
) -> list[dict[str, Any]]:
    paginator = xray_client.get_paginator("get_trace_summaries")

    paginate_kwargs: dict[str, Any] = {
        "StartTime": start_time,
        "EndTime": end_time,
        "FilterExpression": filter_expression,
    }

    summaries: list[dict[str, Any]] = []

    for page in paginator.paginate(**paginate_kwargs):
        summaries.extend(page.get("TraceSummaries", []))

    return summaries


def get_traces(
    xray_client: Any,
    trace_ids: list[str],
) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []

    for batch in chunked(trace_ids):
        response = xray_client.batch_get_traces(TraceIds=batch)
        traces.extend(response.get("Traces", []))

    return traces


def _truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: width - 3] + "..."


def _format_table(rows: list[dict[str, Any]], limit: int = 10) -> list[str]:
    if not rows:
        return ["  (none)"]

    name_width = 40
    header = (
        f"{'Name':<{name_width}} "
        f"{'Count':>7} "
        f"{'Avg':>8} "
        f"{'P95':>8} "
        f"{'Max':>8} "
        f"{'%Req':>7}"
    )

    lines = [header, "-" * len(header)]

    for row in rows[:limit]:
        lines.append(
            f"{_truncate(str(row['name']), name_width):<{name_width}} "
            f"{row['count']:>7} "
            f"{row['avg_ms']:>8.2f} "
            f"{row['p95_ms']:>8.2f} "
            f"{row['max_ms']:>8.2f} "
            f"{row['pct_request']:>6.1f}"
        )

    return lines


def _load_segment_document(
    segment: dict[str, Any],
    trace_id: str | None,
) -> dict[str, Any] | None:
    document = segment.get("Document")
    if not document:
        return None

    try:
        return json.loads(document)
    except json.JSONDecodeError:
        logger.warning(
            "XRAY: Could not parse segment document for trace %s",
            trace_id,
        )
        return None


def _update_trace_bounds(
    parsed_segment: dict[str, Any],
    trace_min_start: float | None,
    trace_max_end: float | None,
) -> tuple[float | None, float | None]:
    segment_start = parsed_segment.get("start_time")
    segment_end = parsed_segment.get("end_time")

    if segment_start is not None:
        trace_min_start = (
            segment_start
            if trace_min_start is None
            else min(trace_min_start, segment_start)
        )

    if segment_end is not None:
        trace_max_end = (
            segment_end if trace_max_end is None else max(trace_max_end, segment_end)
        )

    return trace_min_start, trace_max_end


def _collect_node_durations(
    parsed_segment: dict[str, Any],
    durations_by_name: dict[str, list[float]],
) -> None:
    nodes = [parsed_segment, *walk_subsegments(parsed_segment)]

    for node in nodes:
        name = normalise_subsegment_name(node)
        duration_ms = subsegment_duration_ms(node)

        if duration_ms > 0:
            durations_by_name[name].append(duration_ms)


def _parse_trace(trace: dict[str, Any]) -> tuple[float | None, dict[str, list[float]]]:
    """
    Returns a tuple like:
    (
        700,
        {
            "CampaignRepo": [250],
            "Lambda": [390],
            "SecretRepo": [30],
        }
    )
    """
    trace_id = trace.get("Id")
    durations_by_name: dict[str, list[float]] = defaultdict(list)

    trace_min_start: float | None = None
    trace_max_end: float | None = None

    for segment in trace.get("Segments", []) or []:
        parsed_segment = _load_segment_document(segment, trace_id)
        if parsed_segment is None:
            continue

        # this is calculating the overall trace duration
        # by finding the earliest start time and latest end time
        trace_min_start, trace_max_end = _update_trace_bounds(
            parsed_segment,
            trace_min_start,
            trace_max_end,
        )
        _collect_node_durations(parsed_segment, durations_by_name)

    if trace_min_start is None or trace_max_end is None:
        return None, durations_by_name

    trace_response_ms = (trace_max_end - trace_min_start) * 1000.0
    return trace_response_ms, durations_by_name


def collect_xray_metrics(
    start_time: datetime,
    end_time: datetime,
    region_name: str,
    filter_expression: str = "",
) -> dict[str, Any]:
    logger.info(
        "XRAY: Collecting traces from %s to %s filter=%r",
        start_time.isoformat(),
        end_time.isoformat(),
        filter_expression,
    )
    xray_client = boto3.client("xray", region_name=region_name)  # NOSONAR

    summaries = get_trace_summaries(
        xray_client,
        start_time=start_time,
        end_time=end_time,
        filter_expression=filter_expression,
    )

    trace_ids = [summary["Id"] for summary in summaries if "Id" in summary]
    if not trace_ids:
        return {
            "trace_count": 0,
            "avg_trace_response_ms": 0.0,
            "p95_trace_response_ms": 0.0,
            "slowest_trace_id": None,
            "slowest_trace_response_ms": 0.0,
            "init_row": None,
            "invoke_row": None,
            "app_rows_by_avg": [],
        }

    traces = get_traces(
        xray_client,
        trace_ids,
    )

    trace_response_times_ms: list[float] = []
    subsegment_durations: dict[str, list[float]] = defaultdict(list)

    slowest_trace_id: str | None = None
    slowest_trace_response_ms = 0.0

    for trace in traces:
        trace_id = trace.get("Id")
        trace_response_ms, durations_by_name = _parse_trace(trace)

        for name, durations in durations_by_name.items():
            # this is building a list of durations for each
            # subsegment name across all traces,
            # e.g. "CampaignRepo": [250, 300, 200, ...]
            subsegment_durations[name].extend(durations)

        if trace_response_ms is None:
            continue

        trace_response_times_ms.append(trace_response_ms)

        if trace_response_ms > slowest_trace_response_ms:
            slowest_trace_response_ms = trace_response_ms
            slowest_trace_id = trace_id

    avg_trace_response_ms = (
        sum(trace_response_times_ms) / len(trace_response_times_ms)
        if trace_response_times_ms
        else 0.0
    )

    all_rows = []
    app_rows = []

    for name, durations in subsegment_durations.items():
        total_ms = sum(durations)
        count = len(durations)
        avg_ms = total_ms / count
        p95_ms = percentile(durations, 95)
        max_ms = max(durations)
        pct_request = (
            (avg_ms / avg_trace_response_ms * 100) if avg_trace_response_ms else 0.0
        )

        row = {
            "name": name,
            "count": count,
            "avg_ms": avg_ms,
            "p95_ms": p95_ms,
            "max_ms": max_ms,
            "pct_request": pct_request,
        }

        all_rows.append(row)

        if is_application_subsegment(name):
            app_rows.append(row)

    # sorts the application segments for the table later
    app_rows_by_avg = sorted(app_rows, key=lambda row: row["avg_ms"], reverse=True)
    # find the cold starts
    init_row = next((row for row in all_rows if row["name"] == "Init"), None)
    # find all lambda executions
    lambda_row = next((row for row in all_rows if row["name"] == "local:Lambda"), None)

    return {
        "trace_count": len(traces),
        "avg_trace_response_ms": avg_trace_response_ms,
        "p95_trace_response_ms": percentile(trace_response_times_ms, 95),
        "slowest_trace_id": slowest_trace_id,
        "slowest_trace_response_ms": slowest_trace_response_ms,
        "init_row": init_row,
        "lambda_row": lambda_row,
        "app_rows_by_avg": app_rows_by_avg,
    }


def log_xray_metrics(
    metrics: dict[str, Any],
    start_time: datetime,
    end_time: datetime,
    filter_expression: str,
    limit: int = 10,
) -> None:
    lines = [
        "=" * 60,
        "XRAY APPLICATION TRACE SUMMARY",
        "=" * 60,
        f"Window:           {start_time.isoformat()} -> {end_time.isoformat()}",
        f"Filter:           {filter_expression}",
    ]

    lines.extend(
        [
            "",
            f"Traces analysed:  {metrics['trace_count']}",
            f"Avg trace time:   {metrics['avg_trace_response_ms']:.2f} ms",
            f"P95 trace time:   {metrics['p95_trace_response_ms']:.2f} ms",
            f"Slowest trace:    {metrics['slowest_trace_id']}",
            f"Slowest trace ms: {metrics['slowest_trace_response_ms']:.2f} ms",
            "",
            "Platform overhead",
            "-----------------",
        ]
    )

    init_row = metrics.get("init_row")
    lambda_row = metrics.get("lambda_row")

    if init_row:
        lines.append(
            f"Cold starts (Init): count={init_row['count']} avg={init_row['avg_ms']:.2f} ms"
        )

    if lambda_row:
        lines.append(
            f"Lambda runtime:      count={lambda_row['count']} avg={lambda_row['avg_ms']:.2f} ms"
        )

    lines.extend(
        [
            "",
            "-" * 60,
            "TOP APPLICATION SUBSEGMENTS BY AVG TIME",
            "-" * 60,
            *_format_table(metrics.get("app_rows_by_avg", []), limit=limit),
        ]
    )

    logger.info("\n%s", "\n".join(lines))


def write_xray_metrics_to_file(metrics, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
