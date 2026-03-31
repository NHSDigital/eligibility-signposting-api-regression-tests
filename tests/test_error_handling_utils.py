import os
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from utils.data_helper import load_all_expected_responses, load_all_test_scenarios
from utils.dynamo_helper import file_backup_exists
from utils.eligibility_api_client import EligibilityApiClient
from tests.performance_tests.validate_inputs import parse_run_time_to_seconds

# ---------------------------------------------------------------------------
# 1. data_helper.py — load_all_expected_responses
# ---------------------------------------------------------------------------


def test_load_all_expected_responses_skips_bad_json(tmp_path):
    """A corrupt JSON file is skipped; valid files in the same folder still load."""
    (tmp_path / "bad.json").write_text("NOT JSON", encoding="utf-8")
    (tmp_path / "good.json").write_text('{"id": "123", "data": "ok"}', encoding="utf-8")

    result = load_all_expected_responses(tmp_path)

    assert "good.json" in result
    assert "bad.json" not in result


# ---------------------------------------------------------------------------
# 2. data_helper.py — load_all_test_scenarios
# ---------------------------------------------------------------------------


def test_load_all_test_scenarios_skips_missing_data_key(tmp_path):
    """A scenario file that has no 'data' key is skipped; the result is an empty dict.

    The TemplateEngine is mocked so that no real template files are needed.
    Its apply() return value is configured to return the input unchanged so
    that placeholder resolution does not fail if the 'data' key were present.
    """
    (tmp_path / "scenario.json").write_text('{"other_key": "value"}', encoding="utf-8")

    mock_engine = MagicMock()
    mock_engine.apply.side_effect = lambda data: data  # pass-through

    with patch("utils.data_helper.TemplateEngine.create", return_value=mock_engine):
        result = load_all_test_scenarios(tmp_path)

    assert result == {}


def test_load_all_test_scenarios_skips_bad_json(tmp_path):
    """A scenario file with invalid JSON is skipped; the result is an empty dict."""
    (tmp_path / "scenario.json").write_text("{ bad json", encoding="utf-8")

    mock_engine = MagicMock()
    mock_engine.apply.side_effect = lambda data: data

    with patch("utils.data_helper.TemplateEngine.create", return_value=mock_engine):
        result = load_all_test_scenarios(tmp_path)

    assert result == {}


# ---------------------------------------------------------------------------
# 3. dynamo_helper.py — file_backup_exists
# ---------------------------------------------------------------------------


@patch("utils.dynamo_helper.load_from_file")
def test_file_backup_exists_catches_json_decode_error(mock_load_from_file):
    """file_backup_exists returns False (not an exception) when a backup file is corrupted."""
    # All load_from_file calls return a string that is not valid JSON
    mock_load_from_file.return_value = "NOT JSON"

    mock_db = MagicMock()
    mock_db.environment = "test"

    result = file_backup_exists(mock_db)

    assert result is False


@patch.dict(os.environ, {"BASE_URL": "http://localhost"}, clear=True)
@patch("utils.eligibility_api_client.boto3.client")
def test_api_client_handles_ssm_client_error(mock_boto_client):
    """A boto3 ClientError from SSM is wrapped in a descriptive RuntimeError."""
    mock_ssm = MagicMock()
    error_response = {
        "Error": {"Code": "AccessDeniedException", "Message": "Access Denied"}
    }
    mock_ssm.get_parameter.side_effect = ClientError(error_response, "GetParameter")
    mock_boto_client.return_value = mock_ssm

    # Force _ensure_certs_present to think no certs exist on disk
    with patch("pathlib.Path.exists", return_value=False):
        with pytest.raises(RuntimeError, match="Error retrieving .* from SSM"):
            EligibilityApiClient()


# ---------------------------------------------------------------------------
# 4. validate_inputs.py — parse_run_time_to_seconds and main()
# ---------------------------------------------------------------------------


def test_parse_run_time_to_seconds_valid():
    """Seconds and minutes are parsed correctly."""
    assert parse_run_time_to_seconds("30s") == 30
    assert parse_run_time_to_seconds("5m") == 300


@patch("tests.performance_tests.validate_inputs.fail")
def test_parse_run_time_to_seconds_invalid(mock_fail):
    """An unsupported unit calls fail() with the right message and returns 0 (not an exception)."""
    result = parse_run_time_to_seconds("10h")

    mock_fail.assert_called_once_with(
        "Invalid run_time format",
        "run_time must look like 30s or 5m (seconds or minutes only). Got: '10h'",
    )
    # After fail() is mocked (no sys.exit), the function returns the sentinel 0
    assert result == 0


@patch("tests.performance_tests.validate_inputs.get_env")
@patch("tests.performance_tests.validate_inputs.fail")
def test_validate_inputs_main_catches_value_error(mock_fail, mock_get_env):
    """A non-integer USERS value calls fail() and returns early — no NameError or crash."""
    from tests.performance_tests.validate_inputs import main

    # USERS returns a non-integer; all other env vars return a valid integer string
    mock_get_env.side_effect = lambda name: "NOT_AN_INT" if name == "USERS" else "1"

    # Should complete without raising any exception
    main()

    mock_fail.assert_called_once_with("Invalid input", "USERS must be an integer.")
