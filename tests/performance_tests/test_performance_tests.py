import csv
import logging
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import boto3
import pytest

from tests import test_config
from utils.data_helper import initialise_tests
from utils.s3_config_manager import upload_consumer_mapping_file_to_s3


SLA_MAX_MS = 600
SLA_AVG_MS = 200
CW_REGION = "eu-west-2"  # NOSONAR
CW_LOG_GROUP = "/aws/apigateway/default-eligibility-signposting-api"
LOCUST_FILE = "tests/performance_tests/locust.py"
LOCUST_CSV_PREFIX = "temp/locust_results"
LOCUST_HTML_REPORT = "temp/locust_report.html"
AWS_HTML_REPORT = "temp/aws_logs_report.html"
CW_INGESTION_WAIT_S = 150
CW_QUERY_POLL_S = 1

all_data, dto = initialise_tests(test_config.PERFORMANCE_TEST_DATA)
config_path = test_config.PERFORMANCE_TEST_CONFIGS
upload_consumer_mapping_file_to_s3(test_config.CONSUMER_MAPPING_FILE)

param_list = list(all_data.items())
id_list = [
    f"{filename} - {scenario.get('scenario_name', 'No Scenario')}"
    for filename, scenario in param_list
]


def _epoch_now() -> int:
    return int(datetime.now().timestamp())


def _build_locust_command(
    perf_users: str,
    perf_spawn_rate: str,
    perf_run_time: str,
    csv_prefix: str,
    html_report: str,
) -> list[str]:
    return [
        "locust",
        "-f",
        LOCUST_FILE,
        "--headless",
        "-u",
        str(perf_users),
        "-r",
        str(perf_spawn_rate),
        "-t",
        str(perf_run_time),
        "--csv",
        csv_prefix,
        "--html",
        html_report,
        "--stop-timeout",
        "30",
    ]


def _run_locust(
    command: list[str], env: Dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, env=env)


def _read_locust_aggregated_stats(stats_file: Path) -> Dict[str, float]:
    """
    Returns dict with keys: avg, min, max, failures
    """
    with stats_file.open(mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Name") == "Aggregated":
                return {
                    "avg": float(row["Average Response Time"]),
                    "min": float(row["Min Response Time"]),
                    "max": float(row["Max Response Time"]),
                    "failures": int(row["Failure Count"]),
                }

    raise AssertionError(f"Could not find 'Aggregated' row in {stats_file}")


def _warn_on_locust_sla(
    locust_stats: Dict[str, float], sla_ms: float = SLA_AVG_MS
) -> None:
    failures = int(locust_stats.get("failures", 0))
    avg = float(locust_stats.get("avg", 0.0))

    if failures != 0:
        logging.warning("Test had %s failures. Full stats: %s", failures, locust_stats)

    if avg > sla_ms:
        logging.warning(
            "LOCUST LOGS: Average response time was %.2fms (Max allowed: %.0fms). Full stats: %s",
            avg,
            sla_ms,
            locust_stats,
        )


def _logs_insights_query_string() -> str:
    return (
        "stats avg(integrationLatency) as avgIntegrationLatency,"
        " max(integrationLatency) as maxIntegrationLatency,"
        " min(integrationLatency) as minIntegrationLatency,"
        " avg(responseLatency) as avgResponseLatency,"
        " max(responseLatency) as maxResponseLatency,"
        " min(responseLatency) as minResponseLatency,"
        " count_distinct(requestId) as recordCount"
    )


def _run_logs_insights_query(
    client,
    *,
    log_group: str,
    start_time: int,
    end_time: int,
    query: str,
    poll_interval_s: int = CW_QUERY_POLL_S,
    timeout_s: int = 60,
) -> dict:
    response = client.start_query(
        logGroupName=log_group,
        startTime=start_time,
        endTime=end_time,
        queryString=query,
    )
    query_id = response["queryId"]

    deadline = time.time() + timeout_s
    while True:
        result = client.get_query_results(queryId=query_id)
        if result.get("status") == "Complete":
            return result

        if time.time() >= deadline:
            raise AssertionError(
                "CloudWatch Logs Insights query timed out. "
                f"Status={result.get('status')} start_time={start_time} end_time={end_time} "
                f"query_id={query_id}"
            )

        time.sleep(poll_interval_s)


def _parse_aws_log_stats(insights_result: dict) -> Dict[str, float]:
    if not insights_result.get("results"):
        raise AssertionError(
            "CloudWatch Logs Insights returned no rows. "
            f"Status={insights_result.get('status')}"
        )

    row = insights_result["results"][0]
    row_dict = {field["field"]: field.get("value") for field in row}

    return {
        "avg_integration": float(row_dict.get("avgIntegrationLatency") or 0.0),
        "min_integration": float(row_dict.get("minIntegrationLatency") or 0.0),
        "max_integration": float(row_dict.get("maxIntegrationLatency") or 0.0),
        "avg_response": float(row_dict.get("avgResponseLatency") or 0.0),
        "min_response": float(row_dict.get("minResponseLatency") or 0.0),
        "max_response": float(row_dict.get("maxResponseLatency") or 0.0),
        "record_count": int(float(row_dict.get("recordCount") or 0.0)),
    }


def _warn_on_aws_sla(
    aws_log_stats: Dict[str, float],
    avg_sla_ms: float = SLA_AVG_MS,
    max_sla_ms: float = SLA_MAX_MS,
) -> None:
    if aws_log_stats["record_count"] <= 0:
        logging.warning(
            "No CloudWatch log records found. Stats: %s",
            aws_log_stats,
        )

    checks = [
        ("avg_integration", "Average integration latency", avg_sla_ms),
        ("avg_response", "Average response latency", avg_sla_ms),
        ("max_integration", "Max integration latency", max_sla_ms),
        ("max_response", "Max response latency", max_sla_ms),
    ]

    for key, label, threshold in checks:
        if aws_log_stats[key] >= threshold:
            logging.warning(
                "CLOUDWATCH LOGS: %s was %sms (Max allowed: %.0fms). Stats: %s",
                label,
                aws_log_stats[key],
                threshold,
                aws_log_stats,
            )


def write_request_params_to_csv(
    nhs_number: str, request_headers: str, csv_path: Path
) -> None:
    file_exists = csv_path.exists()
    with csv_path.open(mode="a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["NhsNumber", "RequestHeaders"])
        writer.writerow([nhs_number, request_headers])


@pytest.fixture(scope="function")
def temp_csv_path() -> Path:
    temp_dir = Path("temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir / "nhs_numbers.csv"


@pytest.fixture(scope="function")
def test_data(get_scenario_params, temp_csv_path) -> None:
    for _, scenario in param_list:
        (
            nhs_number,
            _config_filenames,
            request_headers,
            _query_params,
            _expected_response_code,
        ) = get_scenario_params(scenario, config_path)

        write_request_params_to_csv(nhs_number, request_headers, temp_csv_path)


def test_locust_run_and_csv_exists(
    test_data, eligibility_client, perf_run_time, perf_users, perf_spawn_rate
):
    custom_env = os.environ.copy()
    custom_env["BASE_URL"] = eligibility_client.api_url

    locust_command = _build_locust_command(
        perf_users=perf_users,
        perf_spawn_rate=perf_spawn_rate,
        perf_run_time=perf_run_time,
        csv_prefix=LOCUST_CSV_PREFIX,
        html_report=LOCUST_HTML_REPORT,
    )

    start_time = _epoch_now()
    logging.warning("LOCUST TEST STARTING: start_time=%s", start_time)

    proc = _run_locust(locust_command, env=custom_env)

    end_time = _epoch_now()
    logging.warning("LOCUST TEST FINISHED: end_time=%s", end_time)

    assert proc.returncode == 0, f"Locust failed: {proc.stderr}"

    stats_file = Path(f"{LOCUST_CSV_PREFIX}_stats.csv")
    assert stats_file.exists(), f"Locust stats CSV not found: {stats_file}"

    locust_stats = _read_locust_aggregated_stats(stats_file)
    _warn_on_locust_sla(locust_stats, sla_ms=SLA_AVG_MS)

    # CloudWatch logs can arrive late
    time.sleep(CW_INGESTION_WAIT_S)

    logs_client = boto3.client("logs", region_name=CW_REGION)  # NOSONAR
    insights_result = _run_logs_insights_query(
        logs_client,
        log_group=CW_LOG_GROUP,
        start_time=start_time,
        end_time=end_time,
        query=_logs_insights_query_string(),
        poll_interval_s=CW_QUERY_POLL_S,
        timeout_s=60,
    )

    aws_log_stats = _parse_aws_log_stats(insights_result)

    output_results_html(AWS_HTML_REPORT, locust_stats, aws_log_stats)
    _warn_on_aws_sla(aws_log_stats, avg_sla_ms=SLA_AVG_MS, max_sla_ms=SLA_MAX_MS)


def output_results_html(
    output: str,
    locust_stats,
    aws_log_stats,
):
    html = f"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Performance Results</title>

<style>
  body {{
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    background: #f9fafb;
    padding: 40px;
  }}

  .card {{
    max-width: 900px;
    margin: auto;
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 28px;
  }}

  h2 {{
    margin: 0 0 6px 0;
    font-size: 20px;
  }}

  .section {{
    margin-top: 28px;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 12px;
  }}

  th {{
    text-align: left;
    background: #f3f4f6;
    font-weight: 600;
    font-size: 13px;
    padding: 10px;
    border-bottom: 1px solid #e5e7eb;
  }}

  td {{
    padding: 10px;
    border-bottom: 1px solid #f1f5f9;
    font-size: 14px;
  }}

  .metric-value {{
    font-weight: 600;
  }}

  .subtle {{
    color: #6b7280;
    font-size: 13px;
  }}
</style>
</head>

<body>
<div class="card">

<h2>Performance Test Results</h2>
<div class="subtle">Locust execution and AWS log analysis</div>

<div class="section">
  <h2>Locust Statistics</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr>
      <td>Average Latency (ms)</td>
      <td class="metric-value">{locust_stats["avg"]:.2f}</td>
    </tr>
    <tr>
      <td>Minimum Latency (ms)</td>
      <td class="metric-value">{locust_stats["min"]:.2f}</td>
    </tr>
    <tr>
      <td>Maximum Latency (ms)</td>
      <td class="metric-value">{locust_stats["max"]:.2f}</td>
    </tr>
    <tr>
      <td>Failures</td>
      <td class="metric-value">{locust_stats["failures"]}</td>
    </tr>
  </table>
</div>

<div class="section">
  <h2>AWS Log Statistics</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr>
      <td>Average Integration Latency (ms)</td>
      <td class="metric-value">{aws_log_stats["avg_integration"]:.2f}</td>
    </tr>
    <tr>
      <td>Min Integration Latency (ms)</td>
      <td class="metric-value">{aws_log_stats["min_integration"]:.2f}</td>
    </tr>
    <tr>
      <td>Maximum Integration Latency (ms)</td>
      <td class="metric-value">{aws_log_stats["max_integration"]:.2f}</td>
    </tr>

    <tr>
      <td>Average Response Latency (ms)</td>
      <td class="metric-value">{aws_log_stats["avg_response"]:.2f}</td>
    </tr>
    <tr>
      <td>Min Response Latency (ms)</td>
      <td class="metric-value">{aws_log_stats["min_response"]:.2f}</td>
    </tr>
    <tr>
      <td>Maximum Response Latency (ms)</td>
      <td class="metric-value">{aws_log_stats["max_response"]:.2f}</td>
    </tr>

    <tr>
      <td>Log Records Analysed</td>
      <td class="metric-value">{aws_log_stats["record_count"]}</td>
    </tr>
  </table>
</div>

</div>
</body>
</html>
"""

    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
