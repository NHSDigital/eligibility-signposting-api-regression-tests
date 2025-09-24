import logging
import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from utils.eligibility_api_client import EligibilityApiClient
from utils.s3_config_manager import upload_configs_to_s3, delete_all_configs_from_s3

load_dotenv()

# Resolve test data path robustly
BASE_DIR = Path(__file__).resolve().parent.parent
DYNAMO_DATA_PATH = BASE_DIR / "data" / "dynamoDB" / "test_data.json"

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption(
        "--env",
        required=True,
        action="store",
        default="",
        help="Specify the environment for testing: 'dev', 'test' or 'preprod'",
    )


def pytest_configure(config):
    env = config.getoption("--env").lower()
    assert env in [
        "dev",
        "test",
        "preprod",
    ], f"env must be dev, test, or preprod but was: {env}"

    logger.info(f"Setting environment variables for: {env}")
    os.environ["BASE_URL"] = (
        f"https://{env}.eligibility-signposting-api.nhs.uk/patient-check/"
    )
    os.environ["S3_BUCKET_NAME"] = f"eligibility-signposting-api-{env}-eli-rules"
    os.environ["SSM_PARAM_KEY_FILE"] = f"/{env}/mtls/api_private_key_cert"
    os.environ["SSM_PARAM_CLIENT_CERT"] = f"/{env}/mtls/api_client_cert"
    os.environ["SSM_PARAM_CA_CERT"] = f"/{env}/mtls/api_ca_cert"
    os.environ["DYNAMODB_TABLE_NAME"] = (
        f"eligibility-signposting-api-{env}-eligibility_datastore"
    )

    assert os.getenv("BASE_URL") is not None, "BASE_URL must be set"
    assert os.getenv("S3_BUCKET_NAME") is not None, "S3_BUCKET_NAME must be set"
    assert os.getenv("SSM_PARAM_KEY_FILE") is not None, "SSM_PARAM_KEY_FILE must be set"
    assert (
        os.getenv("SSM_PARAM_CLIENT_CERT") is not None
    ), "SSM_PARAM_CLIENT_CERT must be set"
    assert os.getenv("SSM_PARAM_CA_CERT") is not None, "SSM_PARAM_CA_CERT must be set"
    assert (
        os.getenv("DYNAMODB_TABLE_NAME") is not None
    ), "DYNAMODB_TABLE_NAME must be set"

    logger.debug(f"Environment variables for the {env.upper()} environment:")
    logger.debug(f"BASE_URL: {os.getenv("BASE_URL")}")
    logger.debug(f"S3_BUCKET_NAME: {os.getenv("S3_BUCKET_NAME")}")
    logger.debug(f"SSM_PARAM_KEY_FILE: {os.getenv("SSM_PARAM_KEY_FILE")}")
    logger.debug(f"SSM_PARAM_CLIENT_CERT: {os.getenv("SSM_PARAM_CLIENT_CERT")}")
    logger.debug(f"SSM_PARAM_CA_CERT: {os.getenv("SSM_PARAM_CA_CERT")}")
    logger.debug(f"DYNAMODB_TABLE_NAME: {os.getenv("DYNAMODB_TABLE_NAME")}")
    return


@pytest.fixture(scope="session")
def eligibility_client():
    return EligibilityApiClient(cert_dir="certs")


@pytest.fixture(scope="session")
def get_scenario_params(request):
    _ = request

    def _setup(scenario, config_path):
        nhs_number = scenario["nhs_number"]
        config_filenames = scenario.get("config_filenames", [])
        request_headers = scenario.get("request_headers", {})
        query_params = scenario.get("query_params", {})
        expected_response_code = scenario["expected_response_code"]

        delete_all_configs_from_s3()
        upload_configs_to_s3(config_filenames, config_path)

        return (
            nhs_number,
            config_filenames,
            request_headers,
            query_params,
            expected_response_code,
        )

    return _setup
