import csv
import os
import subprocess
from pathlib import Path

import pytest

from tests import test_config
from utils.data_helper import initialise_tests
from utils.s3_config_manager import upload_consumer_mapping_file_to_s3
import boto3
from datetime import datetime, timedelta, timezone
import time

# Update the below with the configuration values specified in test_config.py
all_data, dto = initialise_tests(test_config.PERFORMANCE_TEST_DATA)
config_path = test_config.PERFORMANCE_TEST_CONFIGS
upload_consumer_mapping_file_to_s3(test_config.CONSUMER_MAPPING_FILE)

param_list = list(all_data.items())
id_list = [
    f"{filename} - {scenario.get('scenario_name', 'No Scenario')}"
    for filename, scenario in param_list
]


def write_request_params_to_csv(nhs_number: str, request_headers: str, csv_path: Path):
    with csv_path.open(mode="a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not csv_path.exists():
            writer.writerow(["NhsNumber"])
        writer.writerow([nhs_number, request_headers])


@pytest.fixture(scope="function")
def temp_csv_path():
    temp_dir = Path("temp")
    file_path = temp_dir / "nhs_numbers.csv"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return file_path


@pytest.fixture(scope="function")
def test_data(get_scenario_params, temp_csv_path):
    for filename, scenario in param_list:
        (
            nhs_number,
            config_filenames,
            request_headers,
            query_params,
            expected_response_code,
        ) = get_scenario_params(scenario, config_path)
        write_request_params_to_csv(nhs_number, request_headers, temp_csv_path)


def test_locust_run_and_csv_exists(test_data, eligibility_client, perf_run_time, perf_users, perf_spawn_rate):
    custom_env = os.environ.copy()
    custom_env["BASE_URL"] = eligibility_client.api_url
    locust_report = "temp/locust_results"

    start_time = int(datetime.now().timestamp())

    locust_command = [
        "locust",
        "-f",
        "tests/performance_tests/locust.py",
        "--headless",
        "-u",
        perf_users,
        "-r",
        perf_spawn_rate,
        "-t",
        perf_run_time,
        "--csv",
        locust_report,
        "--html",
        "temp/locust_report.html",
    ]

    result = subprocess.run(
        locust_command, capture_output=True, text=True, env=custom_env
    )

    end_time = int(datetime.now().timestamp())

    assert result.returncode == 0, f"Locust failed: {result.stderr}"
    stats_file = Path(f"{locust_report}_stats.csv")

    locust_stats = {}

    with open(stats_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Name"] == "Aggregated":
                locust_stats = {
                    "avg": float(row["Average Response Time"]),
                    "min": float(row["Min Response Time"]),
                    "max": float(row["Max Response Time"]),
                    "failures": int(row["Failure Count"]),
                }
                break

    assert locust_stats["failures"] == 0, (
        f"Test had {locust_stats['failures']} failures. "
        f"Full stats: {locust_stats}"
    )

    #
    # assert locust_stats["avg"] <= 600, (
    #     f"SLA Violated: Average response time was "
    #     f"{locust_stats['avg']:.2f}ms (Max allowed: 600ms). "
    #     f"Full stats: {locust_stats}"
    # )

    time.sleep(100)

    query = (
        "stats avg(integrationLatency) as avgIntegrationLatency,"
        " max(integrationLatency) as maxIntegrationLatency,"
        " min(integrationLatency) as minIntegrationLatency,"
        " avg(responseLatency) as avgResponseLatency,"
        " max(responseLatency) as maxResponseLatency,"
        " min(responseLatency) as minResponseLatency,"
        " count(*) as recordCount"
    )

    client = boto3.client("logs", region_name="eu-west-2")

    print(start_time, end_time)

    response = client.start_query(
        logGroupName="/aws/apigateway/default-eligibility-signposting-api",
        startTime=start_time,
        endTime=end_time,
        queryString=query,
    )

    query_id = response["queryId"]

    # Poll for query completion
    while True:
        result = client.get_query_results(queryId=query_id)
        if result["status"] == "Complete":
            break
        time.sleep(1)

    if not result.get("results"):
        raise AssertionError(
            "CloudWatch Logs Insights returned no rows. "
            f"Status={result.get('status')} "
            f"start_time={start_time} end_time={end_time}"
        )

    # AWS log results
    aws_log_stats = {}

    # log results
    for row in result["results"]:
        row_dict = {field["field"]: field.get("value") for field in row}

        aws_log_stats = {
            "avg_integration": float(row_dict.get("avgIntegrationLatency")),
            "min_integration": float(row_dict.get("minIntegrationLatency")),
            "max_integration": float(row_dict.get("maxIntegrationLatency")),
            "avg_response": float(row_dict.get("avgResponseLatency")),
            "min_response": float(row_dict.get("minResponseLatency")),
            "max_response": float(row_dict.get("maxResponseLatency")),
            "record_count": int(row_dict.get("recordCount")),
        }

        break

    output_results_html("temp/aws_logs_report.html", locust_stats, aws_log_stats)

    assert aws_log_stats["record_count"] > 0, (
        f"No CloudWatch log records found. Stats: {aws_log_stats}"
    )

    assert aws_log_stats["avg_integration"] < 600, (
        f"Average integration latency was "
        f"{aws_log_stats['avg_integration']}ms (Max allowed: 600ms). "
        f"Stats: {aws_log_stats}"
    )

    assert aws_log_stats["max_integration"] < 600, (
        f"Max integration latency was "
        f"{aws_log_stats['max_integration']}ms (Max allowed: 600ms). "
        f"Stats: {aws_log_stats}"
    )

    assert aws_log_stats["avg_response"] < 600, (
        f"Average response latency was "
        f"{aws_log_stats['avg_response']}ms (Max allowed: 600ms). "
        f"Stats: {aws_log_stats}"
    )

    assert aws_log_stats["max_response"] < 600, (
        f"Max response latency was "
        f"{aws_log_stats['max_response']}ms (Max allowed: 600ms). "
        f"Stats: {aws_log_stats}"
    )


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
    <tr><td>Average Latency (ms)</td><td class="metric-value">{locust_stats["avg"]:.2f}</td></tr>
    <tr><td>Minimum Latency (ms)</td><td class="metric-value">{locust_stats["min"]:.2f}</td></tr>
    <tr><td>Maximum Latency (ms)</td><td class="metric-value">{locust_stats["max"]:.2f}</td></tr>
    <tr><td>Failures</td><td class="metric-value">{locust_stats["failures"]}</td></tr>
  </table>
</div>

<div class="section">
  <h2>AWS Log Statistics</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Average Integration Latency (ms)</td><td class="metric-value">{aws_log_stats["avg_integration"]:.2f}</td></tr>
    <tr><td>Min Integration Latency (ms)</td><td class="metric-value">{aws_log_stats["min_integration"]:.2f}</td></tr>
    <tr><td>Maximum Integration Latency (ms)</td><td class="metric-value">{aws_log_stats["max_integration"]:.2f}</td></tr>

    <tr><td>Average Response Latency (ms)</td><td class="metric-value">{aws_log_stats["avg_response"]:.2f}</td></tr>
    <tr><td>Min Response Latency (ms)</td><td class="metric-value">{aws_log_stats["min_response"]:.2f}</td></tr>
    <tr><td>Maximum Response Latency (ms)</td><td class="metric-value">{aws_log_stats["max_response"]:.2f}</td></tr>

    <tr><td>Log Records Analysed</td><td class="metric-value">{aws_log_stats["record_count"]}</td></tr>
  </table>
</div>

</div>
</body>
</html>
"""

    with open(output, "w", encoding="utf-8") as f:
         f.write(html)

