import hashlib
import json
import logging
import os
from pathlib import Path

import boto3
import botocore.exceptions
from dotenv import load_dotenv

from utils.data_helper import resolve_placeholders_in_data
from utils.placeholder_context import PlaceholderDTO

load_dotenv()
logger = logging.getLogger(__name__)

_cached_s3_config_manager: "S3ConfigManager | None" = None


class S3ConfigManager:
    def __init__(self, bucket_name: str) -> None:
        self.bucket_name: str = bucket_name
        self.s3_client = boto3.client("s3")
        # In-memory tracking of what we've uploaded to S3.
        # Maps s3_key -> resolved JSON string. Avoids redundant list/get/put calls
        # when consecutive tests use the same config set.
        self._uploaded_configs: dict[str, str] = {}

    def _s3_key(self, filename: str) -> str:
        return str(Path() / filename)

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Return SHA256 hash of the given file."""
        sha256 = hashlib.sha256()
        with file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def upload_if_missing_or_changed(self, local_path: Path) -> None:
        filename = Path(local_path).name
        s3_key = self._s3_key(filename)

        try:
            if self.config_exists_and_matches(local_path, s3_key):
                logger.debug(
                    "\n🔍 Config '%s' already exists and matches in S3. Skipping upload.",
                    filename,
                )
                return
            logger.debug("🧹 A different config exists. Deleting all existing files...")
            self.delete_all()
        except self.s3_client.exceptions.NoSuchKey:
            logger.debug("🆕 No config found. Proceeding to upload.")
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                logger.debug("🆕 No config found. Proceeding to upload.")
            else:
                raise

        logger.debug("⬆️ Uploading new config '%s' to S3...", filename)
        self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
        logger.debug("📄 Uploaded to s3://%s/%s", self.bucket_name, s3_key)

    def config_exists_and_matches(self, local_path: Path, s3_key: str) -> bool:
        session = boto3.Session()
        credentials = session.get_credentials()
        assert credentials.access_key is not None, "aws_access_key_id not set"
        assert credentials.secret_key is not None, "aws_secret_access_key not set"
        assert credentials.token is not None, "aws_token not set"

        try:
            s3_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            s3_data = s3_obj["Body"].read()
            s3_hash = hashlib.sha256(s3_data).hexdigest()
            local_hash = self._calculate_file_hash(local_path)
        except self.s3_client.exceptions.NoSuchKey:
            return False
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                return False
            raise
        else:
            return s3_hash == local_hash

    def delete_all(self) -> None:
        """Delete all S3 objects"""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

        if "Contents" in response:
            to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            self.s3_client.delete_objects(
                Bucket=self.bucket_name, Delete={"Objects": to_delete}
            )
            logger.debug("🗑️ Deleted %d file(s).", len(to_delete))
        else:
            logger.warning("📭 Nothing to delete.")
        self._uploaded_configs.clear()

    def upload_all_configs(self, local_paths: list[Path]) -> None:
        desired_filenames = [p.name for p in local_paths]
        desired_keys = {self._s3_key(name) for name in desired_filenames}

        # Resolve all configs locally first
        dto = PlaceholderDTO()
        resolved_configs: dict[str, str] = {}
        for path in local_paths:
            filename = path.name
            s3_key = self._s3_key(filename)

            logger.debug("🔧 Resolving placeholders in config: %s", filename)

            with path.open() as f:
                raw_data = json.load(f)

            resolved = resolve_placeholders_in_data(raw_data, dto, filename)
            resolved_configs[s3_key] = json.dumps(resolved, indent=2)

        # Check if the desired state matches what we've already uploaded
        if (
            self._uploaded_configs
            and set(self._uploaded_configs.keys()) == desired_keys
            and all(
                self._uploaded_configs.get(k) == resolved_configs.get(k)
                for k in desired_keys
            )
        ):
            logger.debug("⏭️ S3 configs unchanged since last upload. Skipping.")
            return

        # Delete stale keys not in the desired set
        if self._uploaded_configs:
            # Use in-memory state — no list API call needed
            stale_keys = [k for k in self._uploaded_configs if k not in desired_keys]
        else:
            # First call — check S3 for any pre-existing objects
            existing_keys = self._list_existing_keys()
            stale_keys = [k for k in existing_keys if k not in desired_keys]

        if stale_keys:
            self._delete_keys(stale_keys)
            for k in stale_keys:
                self._uploaded_configs.pop(k, None)

        # Upload configs that are new or changed
        for s3_key, resolved_json_str in resolved_configs.items():
            if self._uploaded_configs.get(s3_key) == resolved_json_str:
                logger.debug("✅ Config '%s' unchanged. Skipping upload.", s3_key)
            else:
                logger.debug("⬆️ Uploading config '%s' to S3...", s3_key)
                self.s3_client.put_object(
                    Body=resolved_json_str.encode("utf-8"),
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    ContentType="application/json",
                )
                logger.debug("📄 Uploaded to s3://%s/%s", self.bucket_name, s3_key)
                self._uploaded_configs[s3_key] = resolved_json_str

    def config_exists_and_matches_str(self, local_json_str: str, s3_key: str) -> bool:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            remote_str = response["Body"].read().decode("utf-8")
            return local_json_str.strip() == remote_str.strip()
        except self.s3_client.exceptions.NoSuchKey:
            return False

    def _list_existing_keys(self) -> list[str]:
        """List all object keys."""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        return [obj["Key"] for obj in response.get("Contents", [])]

    def _delete_keys(self, keys: list[str]) -> None:
        """Delete specific keys from the S3 bucket."""
        self.s3_client.delete_objects(
            Bucket=self.bucket_name,
            Delete={"Objects": [{"Key": key} for key in keys]},
        )
        logger.debug("🗑️ Deleted %d obsolete file(s): %s", len(keys), keys)


def upload_config_to_s3(local_path: Path) -> None:
    s3_connection = S3ConfigManager(os.getenv("S3_CONFIG_BUCKET_NAME"))
    s3_connection.upload_if_missing_or_changed(local_path)


def upload_configs_to_s3(
    config_files: list[str], config_path: str | Path | None = None
) -> None:
    global _cached_s3_config_manager

    if config_path:
        base = Path(config_path)
        # Treat entries as filenames relative to the base path
        local_paths = [base / f for f in config_files]
    else:
        # Treat entries as fully-qualified paths
        local_paths = [Path(p) for p in config_files]

    bucket = os.getenv("S3_CONFIG_BUCKET_NAME")
    if (
        _cached_s3_config_manager is None
        or _cached_s3_config_manager.bucket_name != bucket
    ):
        _cached_s3_config_manager = S3ConfigManager(bucket)

    _cached_s3_config_manager.upload_all_configs(local_paths)


def delete_all_configs_from_s3() -> None:
    s3_connection = S3ConfigManager(os.getenv("S3_CONFIG_BUCKET_NAME"))
    s3_connection.delete_all()
    # Also invalidate the cached manager so it doesn't think configs still exist
    if _cached_s3_config_manager is not None:
        _cached_s3_config_manager._uploaded_configs.clear()


def upload_consumer_mapping_file_to_s3(local_path: str) -> None:
    s3_bucket = os.getenv("S3_CONSUMER_MAPPING_BUCKET_NAME")
    logger.info(
        "Uploading consumer mapping file: %s to S3 bucket: %s",
        local_path,
        s3_bucket,
    )
    s3_connection = S3ConfigManager(s3_bucket)
    s3_connection.upload_if_missing_or_changed(Path(local_path))
