import os

import boto3
from typing import Optional
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SecretsManagerClient:

    def __init__(self, region: str):
        self.region = region
        self.client = boto3.client("secretsmanager", region_name=region)

    def _get_secret_key_versions(self, secret_name: str) -> dict[str, Optional[bytes]]:

        stages = ["AWSCURRENT", "AWSPREVIOUS"]
        results: dict[str, Optional[bytes]] = {
            "AWSCURRENT": None,
            "AWSPREVIOUS": None,
        }

        for stage in stages:
            try:
                response = self.client.get_secret_value(
                    SecretId=secret_name, VersionStage=stage
                )

                if "SecretString" in response and response["SecretString"]:
                    results[stage] = response["SecretString"].encode()
                elif "SecretBinary" in response and response["SecretBinary"]:
                    results[stage] = response["SecretBinary"]
                else:
                    logger.warning(
                        "Secret '%s' (%s) has no usable value", secret_name, stage
                    )

            except self.client.exceptions.ResourceNotFoundException:
                logger.warning(
                    "Secret '%s' with stage '%s' not found", secret_name, stage
                )

            except Exception as e:
                logger.exception(
                    "Error retrieving '%s' (%s): %s",
                    secret_name,
                    stage,
                    e,
                )

        # Fatal error if both missing
        if not results["AWSCURRENT"] and not results["AWSPREVIOUS"]:
            logger.critical(
                "Fatal: Neither AWSCURRENT nor AWSPREVIOUS exists for '%s'", secret_name
            )
            raise RuntimeError(
                f"No AWSCURRENT or AWSPREVIOUS secret exists for '{secret_name}'"
            )

        return results

    def _set_secret_versions(
        self, secret_name: str, current_value: str, previous_value: str
    ) -> None:
        """
        Safely set AWSCURRENT and AWSPREVIOUS to specified values.
        """
        try:
            # Fetch existing values
            existing = self._get_secret_key_versions(secret_name)
            existing_current_value = (
                existing["AWSCURRENT"].decode() if existing["AWSCURRENT"] else None
            )
            existing_previous_value = (
                existing["AWSPREVIOUS"].decode() if existing["AWSPREVIOUS"] else None
            )

            update_current = existing_current_value != current_value
            update_previous = existing_previous_value != previous_value

            if not update_current and not update_previous:
                logger.info(
                    "Secrets for '%s' unchanged; no new versions created", secret_name
                )
                return

            # --- CREATE NEW VERSION FOR CURRENT ---
            if update_current:
                current_version_id = self.client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=current_value,
                    VersionStages=["AWSCURRENT"],  # attach label explicitly
                )["VersionId"]
                logger.info(
                    "AWSCURRENT updated to version %s for '%s'",
                    current_version_id,
                    secret_name,
                )

            # --- CREATE NEW VERSION FOR PREVIOUS ---
            if update_previous:
                previous_version_id = self.client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=previous_value,
                    VersionStages=["AWSPREVIOUS"],  # attach label explicitly
                )["VersionId"]
                logger.info(
                    "AWSPREVIOUS updated to version %s for '%s'",
                    previous_version_id,
                    secret_name,
                )

        except Exception as e:
            logger.exception(
                "Failed to set secret versions for '%s': %s", secret_name, e
            )
            raise

    def initialise_secret_keys(
        self,
        secret_name: str,
        current_value: Optional[str] = "current_value",
        previous_value: Optional[str] = "previous_value",
    ) -> dict[str, Optional[bytes]]:

        environment = os.getenv("ENVIRONMENT")

        if environment in ("dev", "test"):
            logger.info("Setting AWS Secrets")
            self._set_secret_versions(
                secret_name=secret_name,
                current_value=f"{current_value}_{os.getenv('ENVIRONMENT')}",
                previous_value=f"{previous_value}_{os.getenv('ENVIRONMENT')}",
            )
        else:
            logger.warning(
                f"{os.getenv("ENVIRONMENT")} is not supported. Using existing AWS secrets instead."
            )

        self._remove_awsprevious(secret_name)

        return self._get_secret_key_versions(secret_name)

    def _remove_awsprevious(self, secret_name: str) -> None:
        """
        Remove the AWSPREVIOUS staging label from a secret version.
        Safe because AWS does not require AWSPREVIOUS to exist.
        """
        meta = self.client.describe_secret(SecretId=secret_name)
        version_map = meta.get("VersionIdsToStages", {})

        # Find which version currently has AWSPREVIOUS
        previous_version_id = next(
            (vid for vid, stages in version_map.items() if "AWSPREVIOUS" in stages),
            None,
        )

        if not previous_version_id:
            logger.info(
                "No AWSPREVIOUS staging label found for '%s'; nothing to remove",
                secret_name,
            )
            return

        # Remove the label
        self.client.update_secret_version_stage(
            SecretId=secret_name,
            VersionStage="AWSPREVIOUS",
            RemoveFromVersionId=previous_version_id,
        )

        logger.info(
            "Removed AWSPREVIOUS from version %s for '%s'",
            previous_version_id,
            secret_name,
        )
