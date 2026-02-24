import hashlib
import hmac
import http
import logging
import os

import pytest

from dotenv import load_dotenv

from utils.dynamo_helper import insert_into_dynamo
from utils.secrets_helper import SecretsManagerClient

load_dotenv()
logger = logging.getLogger(__name__)

test_cases = [
    {
        "scenario": "AWSCURRENT Only - Record not hashed",
        "nhs_number": "9900054001",
        "expected_status": http.HTTPStatus.OK,
        "hashing_used": None,
        "current_only": True,
    },
    {
        "scenario": "AWSCURRENT only - Record hashed with Current",
        "nhs_number": "9900054002",
        "expected_status": http.HTTPStatus.OK,
        "hashing_used": "AWSCURRENT",
        "current_only": True,
    },
    {
        "scenario": "AWSCURRENT and AWSPREVIOUS present - Record hashed with Current",
        "nhs_number": "9900054003",
        "expected_status": http.HTTPStatus.OK,
        "hashing_used": "AWSCURRENT",
        "current_only": False,
    },
    {
        "scenario": "AWSCURRENT and AWSPREVIOUS present - Record hashed with Previous",
        "nhs_number": "9900054003",
        "expected_status": http.HTTPStatus.OK,
        "hashing_used": "AWSPREVIOUS",
        "current_only": False,
    },
    {
        "scenario": "AWSCURRENT and AWSPREVIOUS present - Record not hashed",
        "nhs_number": "9900054004",
        "expected_status": http.HTTPStatus.NOT_FOUND,
        "hashing_used": None,
        "current_only": False,
    },
]


@pytest.mark.hashingscenarios
@pytest.mark.parametrize(
    "test_case",
    test_cases,
    ids=[tc["scenario"] for tc in test_cases],
)
def test_secret_hashing_nhs_number(eligibility_client, test_case, hashing_secret):

    secrets = hashing_secret(current_only=test_case["current_only"])

    hashing_used = test_case["hashing_used"]
    nhs_number = test_case["nhs_number"]
    request_headers = {
        "nhs-login-nhs-number": f"{nhs_number}",
        "NHSE-Product-ID": "Story_Test_Consumer_ID",
    }
    data = {"NHS_NUMBER": f"{nhs_number}", "ATTRIBUTE_TYPE": "PERSON"}

    if hashing_used:
        secret_key = secrets[hashing_used]
        hashed_nhs_number = hmac.new(
            secret_key, nhs_number.encode(), hashlib.sha512
        ).hexdigest()
        data["NHS_NUMBER"] = hashed_nhs_number

    # insert data
    insert_into_dynamo([data])

    response = eligibility_client.make_request(
        nhs_number,
        headers=request_headers,
        raise_on_error=False,
    )

    assert (
        response["status_code"] == test_case["expected_status"]
    ), f"{test_case['scenario']} failed on status code"


@pytest.fixture(scope="module")
def secrets_manager():
    """Provide a pre-initialized SecretsManagerClient."""
    sm_client = SecretsManagerClient("eu-west-2")
    return sm_client


@pytest.fixture
def hashing_secret(secrets_manager):
    """Return a helper to get current secrets for the hashing secret."""

    def _get_secret(current_only=False):
        secret_name = (
            f"eligibility-signposting-api-{os.getenv('ENVIRONMENT')}/hashing_secret"
        )
        return secrets_manager.initialise_secret_keys(
            secret_name, current_only=current_only
        )

    return _get_secret
