import json
import logging
import os
from copy import deepcopy
from pathlib import Path
import hmac
import hashlib

import boto3
from dotenv import load_dotenv

from .dynamo_helper import insert_into_dynamo
from .placeholder_context import PlaceholderDTO, ResolvedPlaceholderContext
from .placeholder_utils import resolve_placeholders

keys_to_ignore = ["responseId", "lastUpdated", "id"]
load_dotenv()
logger = logging.getLogger(__name__)


def initialise_tests(folder):
    folder_path = Path(folder).resolve()
    all_data, dto = load_all_test_scenarios(folder_path)
    # --- Fetch secret with fatal error if missing ---
    logger.info("Retrieving HMAC secret key from Secrets Manager")
    try:
        secret_key = get_secret_key(
            f"eligibility-signposting-api-{os.getenv('ENVIRONMENT')}/hashing_secret",
            "eu-west-2",
        )
    except RuntimeError as e:
        logger.critical("Cannot continue without HMAC secret key: %s", e)
        raise SystemExit(1)

    # --- Encrypt NHS numbers and insert into DynamoDB ---
    logger.info("Encrypting NHS numbers and inserting data into DynamoDB")
    for scenario in all_data.values():
        dynamo_items = scenario["dynamo_items"]
        hashed_dynamo_items = _encrypt_nhs_numbers(dynamo_items, secret_key)
        insert_into_dynamo(hashed_dynamo_items)
    logger.info("Data Added to Dynamo")
    return all_data, dto


def resolve_placeholders_in_data(data, context, file_name):
    if isinstance(data, dict):
        return {
            k: resolve_placeholders_in_data(v, context, file_name)
            for k, v in data.items()
        }
    if isinstance(data, list):
        return [resolve_placeholders_in_data(item, context, file_name) for item in data]
    return resolve_placeholders(data, context, file_name)


def load_test_scenario(file_path):
    with Path.open(file_path) as f:
        raw_data = json.load(f)

    file_name = Path(file_path).name
    context = ResolvedPlaceholderContext()
    resolved_data = resolve_placeholders_in_data(raw_data["data"], context, file_name)

    return {
        "file": file_name,
        "scenario_name": raw_data.get("scenario_name"),
        "data": resolved_data,
        "placeholders": context.all(),  # Now just placeholder → value
    }


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
    dto = PlaceholderDTO()  # Shared across all files

    for path in Path(folder_path).iterdir():
        if path.suffix != ".json":
            continue

        with path.open() as f:
            raw_json = json.load(f)

        resolved_data = resolve_placeholders_in_data(raw_json, dto, path.name)
        cleaned_data = clean_responses(data=resolved_data, ignore_keys=keys_to_ignore)

        all_data[path.name] = {"response_items": cleaned_data}

    return all_data


def load_all_test_scenarios(folder_path):
    all_data = {}
    dto = PlaceholderDTO()  # Shared across all files

    # Sort files alphabetically by filename
    for path in sorted(Path(folder_path).iterdir(), key=lambda p: p.name.lower()):
        if path.suffix != ".json":
            continue

        with path.open() as f:
            raw_json = json.load(f)

        raw_data = raw_json["data"]

        config_filenames = raw_json.get("config_filenames")
        scenario_name = raw_json.get("scenario_name")
        request_headers = raw_json.get("request_headers")
        expected_response_code = raw_json.get("expected_response_code")
        query_params = raw_json.get("query_params")

        # Resolve placeholders with shared DTO
        resolved_data = resolve_placeholders_in_data(raw_data, dto, path.name)

        # Extract NHS number
        nhs_number = extract_nhs_number_from_data(resolved_data)

        # Add resolved scenario
        all_data[path.name] = {
            "dynamo_items": resolved_data,
            "nhs_number": nhs_number,
            "config_filenames": config_filenames,
            "expected_response_code": expected_response_code,
            "request_headers": request_headers,
            "query_params": query_params,
            "scenario_name": scenario_name,
        }

    return all_data, dto


def load_data_items_to_dynamo(folder_path):
    dto = PlaceholderDTO()  # Shared across all files

    for path in Path(folder_path).iterdir():
        if path.suffix != ".json":
            continue

        with path.open() as f:
            raw_json = json.load(f)

        raw_data = raw_json["data"]

        # Resolve placeholders with shared DTO
        resolved_data = resolve_placeholders_in_data(raw_data, dto, path.name)

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


def get_secret_key(secret_name: str, region: str) -> bytes:
    """
    Fetches a secret from AWS Secrets Manager and returns it as bytes.
    Raises a RuntimeError if the secret cannot be retrieved.
    """
    try:
        secrets_client = boto3.client("secretsmanager", region_name=region)
        response = secrets_client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.exception("Failed to fetch secret '%s': %s", secret_name, e)
        raise RuntimeError(f"Failed to fetch secret '{secret_name}'") from e

    if "SecretString" in response and response["SecretString"] is not None:
        return response["SecretString"].encode()
    elif "SecretBinary" in response and response["SecretBinary"] is not None:
        return response["SecretBinary"]
    else:
        logger.error("No usable secret found for '%s'", secret_name)
        raise RuntimeError(f"No usable secret found for '{secret_name}'")
