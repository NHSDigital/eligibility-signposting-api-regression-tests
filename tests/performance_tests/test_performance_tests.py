import csv
import shutil
import subprocess
from pathlib import Path

import pytest

from tests import test_config
from utils.data_helper import initialise_tests, load_all_expected_responses
from utils.s3_config_manager import upload_consumer_mapping_file_to_s3, upload_configs_to_s3

# Update the below with the configuration values specified in test_config.py
all_data, dto = initialise_tests(test_config.PERFORMANCE_TEST_DATA)
config_path = test_config.PERFORMANCE_TEST_CONFIGS
upload_consumer_mapping_file_to_s3(test_config.CONSUMER_MAPPING_FILE)

param_list = list(all_data.items())
id_list = [
    f"{filename} - {scenario.get('scenario_name', 'No Scenario')}"
    for filename, scenario in param_list
]

def write_nhs_number_to_csv(nhs_number: str, csv_path: Path):
    with csv_path.open(mode="a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not csv_path.exists():
            writer.writerow(["NhsNumber"])
        writer.writerow([nhs_number])


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
        write_nhs_number_to_csv(nhs_number, temp_csv_path)


def test_locust_run_and_csv_exists(test_data):

    locust_command = [
        "locust",
        "-f", "tests/performance_tests/locust.py",
        "--headless",
        "-u", "10",
        "-r", "2",
        "-t", "20s",
        "--csv", "temp/locust_results"
    ]

    result = subprocess.run(locust_command, capture_output=True, text=True)

    assert result.returncode == 0, f"Locust failed: {result.stderr}"


