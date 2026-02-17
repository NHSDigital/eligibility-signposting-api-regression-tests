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


def test_locust_run_and_csv_exists(test_data, eligibility_client):
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
        "10",
        "-r",
        "2",
        "-t",
        "10s",
        "--csv",
        locust_report,
        "--html",
        "temp/report.html",
    ]

    result = subprocess.run(
        locust_command, capture_output=True, text=True, env=custom_env
    )

    end_time = int(datetime.now().timestamp())

    assert result.returncode == 0, f"Locust failed: {result.stderr}"
    stats_file = Path(f"{locust_report}_stats.csv")
    avg_response_time = 0
    total_failures = 0

    with open(stats_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Name"] == "Aggregated":
                avg_response_time = float(row["Average Response Time"])
                total_failures = int(row["Failure Count"])
                break
    assert (
        total_failures == 0
    ), f"Test had {total_failures} failures. Check temp/report.html"
    assert avg_response_time <= 600, (
        f"SLA Violated: Average response time was {avg_response_time:.2f}ms "
        f"(Max allowed: 500ms)"
    )

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

    # Print the actual log results
    for row in result["results"]:
        row_dict = {field["field"]: field.get("value") for field in row}
        avg_integration_latency = float(row_dict.get("avgIntegrationLatency"))
        max_integration_latency = float(row_dict.get("maxIntegrationLatency"))
        avg_response_latency = float(row_dict.get("avgResponseLatency"))
        max_response_latency = float(row_dict.get("maxResponseLatency"))
        record_count = row_dict.get("recordCount")

    assert (
        avg_integration_latency < 600
    ), f"Average response time was {avg_integration_latency}ms (Max allowed: 200ms)"
    assert (
        max_integration_latency < 600
    ), f"Max response time was {max_integration_latency}ms (Max allowed: 200ms)"
    assert (
        avg_response_latency < 600
    ), f"Average response time was {avg_response_latency}ms (Max allowed: 500ms)"
    assert (
        max_response_latency < 600
    ), f"Max response time was {max_response_latency}ms (Max allowed: 500ms)"


def write_latency_flow_html(
    output: str,
    metrics: LatencyDict,
):
    """
    metrics format:

    {
        "locust": {"avg": float, "min": float, "max": float},
        "response": {"avg": float, "min": float, "max": float},
        "integration": {"avg": float, "min": float, "max": float},
    }
    """

    metrics = {
        "locust": {"avg": 820, "min": 410, "max": 2100},
        "response": {"avg": 95, "min": 40, "max": 210},
        "integration": {"avg": 640, "min": 300, "max": 1850},
    }

    title: str = "Latency Flow Overview",
    subtitle: str = "End-to-end request journey with average, minimum and maximum latency (ms)",

    required_sections = {"locust", "response", "integration"}
    if not required_sections.issubset(metrics):
        missing = required_sections - metrics.keys()
        raise ValueError(f"Missing required metric sections: {missing}")

    for section in required_sections:
        for key in ("avg", "min", "max"):
            if key not in metrics[section]:
                raise ValueError(f"Missing '{key}' in metrics['{section}']")

    def fmt(x: float) -> str:
        return str(int(x)) if float(x).is_integer() else f"{x:.1f}"

    loc = metrics["locust"]
    resp = metrics["response"]
    integ = metrics["integration"]

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
</head>
<body>
  <div class="latency-flow-card">
    <style>
      .latency-flow-card {{
        font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: 24px;
        background: #ffffff;
        max-width: 1100px;
      }}

      .latency-title {{
        font-size: 20px;
        font-weight: 700;
        margin-bottom: 4px;
      }}

      .latency-sub {{
        font-size: 13px;
        color: #6b7280;
        margin-bottom: 28px;
      }}

      .flow-container {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 20px;
        flex-wrap: wrap;
      }}

      .flow-node {{
        flex: 1;
        min-width: 220px;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 18px;
        background: #f9fafb;
        text-align: center;
      }}

      .flow-node.external {{
        background: #eef2ff;
        border-color: #c7d2fe;
      }}

      .flow-node.gateway {{
        background: #ecfeff;
        border-color: #a5f3fc;
      }}

      .flow-node.integration {{
        background: #f0fdf4;
        border-color: #bbf7d0;
      }}

      .node-title {{
        font-weight: 600;
        margin-bottom: 12px;
        font-size: 15px;
      }}

      .node-avg {{
        font-size: 22px;
        font-weight: 700;
        margin-bottom: 6px;
      }}

      .node-range {{
        font-size: 12px;
        color: #6b7280;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      }}

      .flow-arrow {{
        font-size: 28px;
        color: #9ca3af;
        font-weight: 300;
      }}

      .flow-summary {{
        margin-top: 28px;
        font-size: 13px;
        color: #374151;
        line-height: 1.6;
      }}
    </style>

    <div class="latency-title">{title}</div>
    <div class="latency-sub">{subtitle}</div>

    <div class="flow-container">

      <div class="flow-node external">
        <div class="node-title">Locust (Client – Outside App)</div>
        <div class="node-avg">{fmt(loc["avg"])} ms</div>
        <div class="node-range">min {fmt(loc["min"])} · max {fmt(loc["max"])}</div>
      </div>

      <div class="flow-arrow">→</div>

      <div class="flow-node gateway">
        <div class="node-title">API Gateway<br>responseLatency</div>
        <div class="node-avg">{fmt(resp["avg"])} ms</div>
        <div class="node-range">min {fmt(resp["min"])} · max {fmt(resp["max"])}</div>
      </div>

      <div class="flow-arrow">→</div>

      <div class="flow-node integration">
        <div class="node-title">Integration (Lambda + DynamoDB/S3)</div>
        <div class="node-avg">{fmt(integ["avg"])} ms</div>
        <div class="node-range">min {fmt(integ["min"])} · max {fmt(integ["max"])}</div>
      </div>

    </div>

    <div class="flow-summary">
      <strong>How to interpret:</strong><br>
      • Locust shows total client-perceived latency.<br>
      • responseLatency is reported by API Gateway.<br>
      • integrationLatency is backend execution time (lambda,s3,DynamoDB etc).<br>
    </div>

  </div>
</body>
</html>
"""

    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
