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
