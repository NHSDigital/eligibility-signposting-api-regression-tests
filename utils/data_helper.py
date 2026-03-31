import json
import logging
import os
from copy import deepcopy
from pathlib import Path
import hmac
import hashlib
from typing import Optional

from dotenv import load_dotenv

from .data_template_resolver import TemplateEngine
from .dynamo_helper import insert_into_dynamo
from .placeholder_utils import resolve_placeholders
from .secrets_helper import SecretsManagerClient

keys_to_ignore = ["responseId", "lastUpdated", "id"]
load_dotenv()
AWS_REGION = "eu-west-2"
logger = logging.getLogger(__name__)

_DEFAULT_PRODUCT_ID = "test-Story_Test_Consumer_ID"


def initialise_tests(folder):
    folder_path = Path(folder).resolve()
    all_data = load_all_test_scenarios(folder_path)

    # Skip DynamoDB insertion if data was preloaded in a dedicated step
    if os.getenv("DYNAMO_PRELOADED", "").lower() == "true":
        logger.info("Skipping DynamoDB insertion (data preloaded)")
        return all_data

    _insert_scenarios_into_dynamo(all_data)

    return all_data


def _insert_scenarios_into_dynamo(all_data):
    """Hash NHS numbers (if required) and insert all scenario data into DynamoDB."""
    # Setup AWS Secrets
    secrets_manager = SecretsManagerClient(AWS_REGION)
    secret_keys = secrets_manager.initialise_secret_keys(
        f"eligibility-signposting-api-{os.getenv('ENVIRONMENT')}/hashing_secret"
    )

    all_items = []

    logger.info("Encrypting NHS numbers (if required) and preparing DynamoDB items")
    for scenario in all_data.values():
        # get the scenario data items to be stored in dynamo
        dynamo_items = scenario["dynamo_items"]
        # get the hashing version to be used in the scenario
        scenario_secret_version = scenario["secret_version"]

        # Case 1: No secrets exist OR user explicitly requests PLAINTEXT
        if (
            not secret_keys["AWSCURRENT"] and not secret_keys["AWSPREVIOUS"]
        ) or scenario_secret_version == "PLAINTEXT":
            items_to_insert = dynamo_items

        # Case 2: Hash using AWSCURRENT / AWSPREVIOUS / None
        elif scenario_secret_version in ("AWSCURRENT", "AWSPREVIOUS", None):
            secret = _get_scenario_secret_for_hashing(
                secret_keys, scenario_secret_version
            )
            items_to_insert = _encrypt_nhs_numbers(dynamo_items, secret)

        # Case 3: Unknown secret version
        else:
            raise ValueError(f"Unknown secret_version: {scenario_secret_version}")

        all_items.extend(items_to_insert)

    # Deduplicate items with the same (NHS_NUMBER, ATTRIBUTE_TYPE) key.
    # Some test scenarios share identical DynamoDB data (e.g. same patient,
    # different S3 configs) and batch_writer rejects duplicate keys in a batch.
    seen_keys = set()
    unique_items = []
    for item in all_items:
        key = (str(item.get("NHS_NUMBER", "")), str(item.get("ATTRIBUTE_TYPE", "")))
        if key not in seen_keys:
            seen_keys.add(key)
            unique_items.append(item)

    if len(unique_items) < len(all_items):
        logger.info(
            "Deduplicated %d → %d items (removed %d duplicates)",
            len(all_items),
            len(unique_items),
            len(all_items) - len(unique_items),
        )

    insert_into_dynamo(unique_items)
    logger.info("Data Added to Dynamo")


def preload_all_dynamo_data(folders):
    """Load and insert DynamoDB data from multiple test suite folders at once.

    Uses a single SecretsManager call and batch DynamoDB writes for efficiency.
    """
    combined_data = {}
    for folder in folders:
        folder_path = Path(folder).resolve()
        if not folder_path.exists() or not any(folder_path.glob("*.json")):
            logger.info("Skipping empty folder: %s", folder)
            continue
        all_data = load_all_test_scenarios(folder_path)
        combined_data.update(all_data)

    logger.info("Preloading %d scenarios into DynamoDB", len(combined_data))
    _insert_scenarios_into_dynamo(combined_data)


def resolve_placeholders_in_data(data, file_name):
    if isinstance(data, dict):
        return {k: resolve_placeholders_in_data(v, file_name) for k, v in data.items()}
    if isinstance(data, list):
        return [resolve_placeholders_in_data(item, file_name) for item in data]
    return resolve_placeholders(data, file_name)


def extract_nhs_number_from_data(data):
    def find_nhs(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower().replace("_", "") == "nhsnumber":
                    return v
                if isinstance(v, (dict, list)):
                    result = find_nhs(v)
                    if result:
                        return result
        elif isinstance(obj, list):
            for item in obj:
                result = find_nhs(item)
                if result:
                    return result
        return None

    return find_nhs(data) or "UNKNOWN"


def load_all_expected_responses(folder_path):
    all_data = {}

    for path in Path(folder_path).iterdir():
        if path.suffix != ".json":
            continue

        try:
            with path.open() as f:
                raw_json = json.load(f)
        except (OSError, IOError) as e:
            logger.error("Failed to read expected response file %s: %s", path, e)
            continue
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in expected response file %s: %s", path, e)
            continue

        resolved_data = resolve_placeholders_in_data(raw_json, path.name)
        cleaned_data = clean_responses(data=resolved_data, ignore_keys=keys_to_ignore)

        all_data[path.name] = {"response_items": cleaned_data}

    return all_data


def _ensure_default_product_id(request_headers: dict, cached_test: bool) -> dict:
    """Add the default NHSE-Product-ID header if not already present."""
    if cached_test:
        return request_headers

    product_id = request_headers.get("NHSE-Product-ID")
    if not product_id:
        request_headers["NHSE-Product-ID"] = _DEFAULT_PRODUCT_ID
    else:
        product_id = str(product_id).strip()
        if not product_id.startswith("test-"):
            request_headers["NHSE-Product-ID"] = f"test-{product_id}"
        else:
            request_headers["NHSE-Product-ID"] = product_id
    return request_headers


def _build_test_scenario_entry(raw_json: dict, resolved_data) -> dict:
    """Construct the standard scenario dict from resolved template data."""
    return {
        "dynamo_items": resolved_data,
        "nhs_number": extract_nhs_number_from_data(resolved_data),
        "config_filenames": raw_json.get("config_filenames"),
        "expected_response_code": raw_json.get("expected_response_code"),
        "request_headers": raw_json.get("request_headers") or {},
        "query_params": raw_json.get("query_params"),
        "scenario_name": raw_json.get("scenario_name"),
        "secret_version": raw_json.get("secret_version"),
    }


def _process_single_scenario(
    path: Path, data_builder: TemplateEngine, cached_test: bool
) -> dict | None:
    try:
        with path.open() as f:
            raw_json = json.load(f)
    except (OSError, IOError) as e:
        logger.error("Failed to read test scenario file %s: %s", path, e)
        return None
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in test scenario file %s: %s", path, e)
        return None

    scenario_data = raw_json.get("data")
    if scenario_data is None:
        logger.error("Missing required 'data' key in scenario file %s", path)
        return None

    try:
        templated_data = data_builder.apply(scenario_data)
    except ValueError as e:
        logger.error("Template application failed for %s: %s", path, e)
        return None

    request_headers = raw_json.get("request_headers") or {}
    raw_json["request_headers"] = _ensure_default_product_id(
        request_headers, cached_test
    )

    resolved_data = resolve_placeholders_in_data(templated_data, path.name)

    return _build_test_scenario_entry(raw_json, resolved_data)


def load_all_test_scenarios(folder_path):
    all_data = {}

    data_builder = TemplateEngine.create()
    cached_test = "performance" in folder_path.name.lower()

    # Sort files alphabetically by filename
    for path in sorted(Path(folder_path).iterdir(), key=lambda p: p.name.lower()):
        if path.suffix != ".json":
            continue

        scenario_result = _process_single_scenario(path, data_builder, cached_test)
        if scenario_result is not None:
            all_data[path.name] = scenario_result

    return all_data


def load_data_items_to_dynamo(folder_path):
    for path in Path(folder_path).iterdir():
        if path.suffix != ".json":
            continue

        try:
            with path.open() as f:
                raw_json = json.load(f)
        except (OSError, IOError) as e:
            logger.error("Failed to read data item file %s: %s", path, e)
            continue
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in data item file %s: %s", path, e)
            continue

        raw_data = raw_json.get("data")
        if raw_data is None:
            logger.error("Missing required 'data' key in file %s", path)
            continue

        # Resolve placeholders with shared DTO
        resolved_data = resolve_placeholders_in_data(raw_data, path.name)

        # Insert immediately
        insert_into_dynamo(resolved_data)


def clean_responses(data: dict, ignore_keys: list) -> dict:
    return _mask_volatile_fields(data, ignore_keys)


def _mask_volatile_fields(data, keys_to_mask, placeholder="<ignored>"):
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            # Always recurse first
            if isinstance(value, (dict, list)):
                masked_value = _mask_volatile_fields(value, keys_to_mask, placeholder)
            else:
                masked_value = value
            # Then decide if this key should be masked
            result[key] = placeholder if key in keys_to_mask else masked_value
        return result
    if isinstance(data, list):
        return [_mask_volatile_fields(item, keys_to_mask, placeholder) for item in data]
    return data


def _encrypt_nhs_numbers(
    dynamo_items: list[dict[str, object]], secret_key: bytes
) -> list[dict[str, object]]:
    encrypted_items = deepcopy(dynamo_items)

    for item in encrypted_items:
        if "NHS_NUMBER" in item:
            nhs_number = str(item["NHS_NUMBER"])
            item["NHS_NUMBER"] = hmac.new(
                secret_key, nhs_number.encode(), hashlib.sha512
            ).hexdigest()

    return encrypted_items


def _get_scenario_secret_for_hashing(
    secret_keys: dict[str, bytes], secret_version: Optional[str]
) -> bytes:
    if not secret_version:
        return secret_keys["AWSCURRENT"]

    return secret_keys[secret_version]
